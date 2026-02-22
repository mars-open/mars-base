/*
 * MARS Base - Maintenance Applications for Railway Systems
 *
 * Copyright © 2026 zzeekk (<zach.kull@gmail.com>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package ch.zzeekk.mars.osm

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils.{interpolateVal, isExtension}
import ch.zzeekk.mars.pp.utils.{GeometryCalcUtils, UDFUuidFromString}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_GeomFromGeoJSON
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.linearref.LengthIndexedLine

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * Creating simplified geometries of main tracks for map overview (small zoom values).
 */
class OsmExtractMainRailwayLinesTransformer extends CustomDfTransformer with SmartDataLakeLogger {

  override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    import session.implicits._

    val udfUuidFromString = new UDFUuidFromString().get(Map())
    val operators = options.getOrElse("operators", "").split(',').map(_.trim)
    val linesToExclude = options.getOrElse("linesToExclude", "").split(',').map(_.trim)
    val toleranceSimplifyVw = options.getOrElse("toleranceSimplifyVW", "100").toDouble

    val udfConvert4326to3857 = udf(GeometryCalcUtils.convert4326to3857 _)
    val udfConvert3857to4326 = udf(GeometryCalcUtils.convertTo4326(_, "EPSG:3857"))

    val udfTracksToLines = udf(tracksToLines _).asNondeterministic()
    def createTagWithPrefixFromNumber(col: Column, prefix: String) = when(col.isNotNull, concat(lit(prefix),col))
    def listFilter(col: Column, values: Seq[String]) = if (values.nonEmpty) col.isin(values: _*) else lit(true)
    def listExcludeFilter(col: Column, values: Seq[String]) = if (values.nonEmpty) not(col.isin(values: _*)) else lit(true)

    val dfOut = df
      .where($"properties.railway" === "rail" and $"properties.usage" === "main")
      .withColumn("geometry", udfConvert4326to3857(ST_GeomFromGeoJSON($"geometry")))
      .where(ST_NumPoints($"geometry") > 1) // remove points and empty geometries (if any)
      // cluster by line and operator
      .withColumn("line", $"properties.ref")
      .where($"line".isNotNull) // remove tracks without line reference
      .where(listExcludeFilter($"line", linesToExclude))
      .withColumn("operator", $"properties.operator")
      .where(listFilter($"operator", operators))
      .groupBy($"line", $"operator")
      // combine tracks of the same line into one geometry (e.g. by merging connected LineStrings and then finding the centerline of the merged geometry)
      .agg(collect_list(ST_Force2D($"geometry")).as("tracks"))
      .withColumn("lines", udfTracksToLines($"tracks", concat(lit("line "),$"line",lit(" ("),$"operator",lit(")"))))
      .withColumn("geometry", ST_SimplifyVW(ST_Union($"lines"), lit(toleranceSimplifyVw))) // simplify geometry to reduce size (tolerance in meters)
      .withColumn("tags", array_compact(array(
        createTagWithPrefixFromNumber($"line", "line"),
        $"operator"
      )).as("tags"))
      .select(
        udfUuidFromString(ST_AsText($"geometry")).as("uuid_line"),
        udfConvert3857to4326($"geometry").as("geometry"),
        $"tags"
      )

    dfOut
  }

  def interpolateCoord2D(c1: Coordinate, c2: Coordinate, pos: Double): Coordinate = {
    val fraction = pos / c1.distance(c2)
    new CoordinateXY(
      interpolateVal(c1.x, c2.x, fraction),
      interpolateVal(c1.y, c2.y, fraction)
    )
  }

  def calcNormalVector(segment: LineSegment): Coordinate = {
    val length = segment.getLength
    assert(length > 0, "Segment length must be greater than 0")
    val dx = segment.p1.x - segment.p0.x
    val dy = segment.p1.y - segment.p0.y
    new Coordinate(-dy / length, dx / length) // Rotate 90 degrees to get normal
  }

  def clusterByIntersection(geoms: Seq[LineString], bufferSize: Double): Seq[Seq[LineString]] = {
    @scala.annotation.tailrec
    def createCluster[X](clusterGeoms: Seq[(Geometry,X)], remainingGeoms: Seq[(Geometry,X)]): (Seq[(Geometry,X)], Seq[(Geometry,X)]) = {
      val (intersecting, leftover) = remainingGeoms.partition(
        r => clusterGeoms.exists(_._1.intersects(r._1))
      )
      // check if cluster is finished
      if (intersecting.isEmpty) (clusterGeoms, leftover)
      // otherwise recurse
      else createCluster(clusterGeoms ++ intersecting, leftover)
    }

    // find clusters of intersecting geoms within given buffer size
    val geomsWithBuffer = geoms.map(g => (g.buffer(bufferSize), g)) // add buffer to create intersection between parallel tracks
    val clusters = mutable.Buffer[Seq[(Geometry, LineString)]]() // store clusters of intersecting geoms
    var remainingGeoms = geomsWithBuffer // initialize with all geoms, then iteratively replace with remaining geoms
    while (remainingGeoms.nonEmpty) {
      val (cluster, leftover) = createCluster(remainingGeoms.take(1), remainingGeoms.tail)
      remainingGeoms = leftover
      clusters.append(cluster)
    }
    clusters.toSeq.map(_.map(_._2))
  }

  /**
   * Combine as many LineStrings as possible to create longer LineStrings, but only if they are extensions of each other (i.e. they have the same direction and are connected at the end/start point)
   */
  def mergeLineStrings(geoms: Seq[LineString]): Seq[LineString] = {
    val geomsBidir = geoms ++ geoms.map(_.reverse)
    val geomsByPoint = geomsBidir
      .groupBy(_.getStartPoint)
      .filter(_._2.size > 1) // remove end points of the network
      .view.mapValues(_.sortBy(_.getLength).reverse) // longer geometries first
      .toMap

    // define recursion to combine with next geometry
    @tailrec
    def combineNext(geom: LineString): LineString = {
      val next = geomsByPoint.get(geom.getEndPoint)
      if (next.isDefined) {
        //val nextGeom = next.get.find(g => g != geom && isExtension(geom, g)) // find next longest geometry that is an extension of the current geometry
        val nextGeom = next.get.filter(g => g != geom && isExtension(geom, g))
          .minByOption(g => GeometryCalcUtils.angleDiff(geom, g))
        if (nextGeom.isDefined) combineNext(GeometryCalcUtils.mergeLineStrings(geom, nextGeom.head))
        else geom
      } else geom
    }

    // start with geoms where startPoint has no entry in geomsByPoint
    val startGeoms = geomsBidir
      .filter(g => !geomsByPoint.contains(g.getStartPoint))
      .toBuffer

    // merge geometries
    val mergedGeoms = mutable.Buffer[LineString]()
    while (startGeoms.nonEmpty) {
      val nextGeom = startGeoms.head
      val mergedGeom = combineNext(nextGeom)
      mergedGeoms.append(mergedGeom)
      startGeoms.remove(0)
      val idx = startGeoms.indexWhere(g => mergedGeom.getEndPoint == g.getStartPoint)
      if (idx >= 0) startGeoms.remove(idx)
    }

    mergedGeoms.toSeq
  }

  /**
   * Merge connected LineStrings and find the centerline of the merged geometries.
   * @return
   */
  def tracksToLines(tracks: Seq[Geometry], line: String): Seq[Geometry] = {
    val factory = tracks.head.getFactory
    val mergedTracks = mutable.Set(mergeLineStrings(tracks.map(_.asInstanceOf[LineString])): _*)
    val mergedTracksIdx = new STRtree()
    mergedTracks.foreach(t => mergedTracksIdx.insert(t.getEnvelopeInternal, t))
    val centerLines = mutable.Buffer[LineString]()
    while (mergedTracks.nonEmpty) {
      val longestTrack = mergedTracks.maxBy(_.getLength)
      mergedTracks -= longestTrack
      mergedTracksIdx.remove(longestTrack.getEnvelopeInternal, longestTrack)
      val indexedLine = new LengthIndexedLine(longestTrack)
      val rasterDistance = 100.0
      val rasterCoords = ((0 to (longestTrack.getLength / rasterDistance).toInt).map(_ * rasterDistance) :+ indexedLine.getEndIndex) // include start and end!
        .map(i => (i, indexedLine.extractPoint(i)))
      val matchedTracks = mutable.Map[LineString,Int]()
      val centerLinePoints = rasterCoords.map { case (i, c) =>
        val normalVector = calcNormalVector(new LineSegment(indexedLine.extractPoint(i - 1), indexedLine.extractPoint(i + 1)))
        val rasterNormalVector = new LineSegment(c, new Coordinate(c.getX + normalVector.getX, c.getY + normalVector.getY))
        val perpendicularLine = factory.createLineString(Array(rasterNormalVector.pointAlong(-50), rasterNormalVector.pointAlong(50)))
        val intersectionPoints = mergedTracksIdx.query(perpendicularLine.getEnvelopeInternal).asScala.map(_.asInstanceOf[LineString])
          .flatMap { track =>
            val coord = Option(perpendicularLine.intersection(track).getCoordinate)
            if (coord.nonEmpty) matchedTracks.put(track, matchedTracks.getOrElse(track, 0) + 1)
            coord
          }.toSeq :+ c
        val avgPoint = if (intersectionPoints.size > 1) new Coordinate(intersectionPoints.map(_.x).sum / intersectionPoints.size, intersectionPoints.map(_.y).sum / intersectionPoints.size)
        else c
        avgPoint
      }
      centerLines.append(factory.createLineString(centerLinePoints.toArray))
      mergedTracks --= matchedTracks.filter{ case (t,cnt) => cnt > t.getLength / rasterDistance / 2 }.keys // remove matched tracks that have more than half of their length intersecting with the centerline)
    }

    logger.info(s"finished processing $line, created ${centerLines.size} center lines from ${tracks.size} tracks")
    centerLines.toSeq
  }
}
