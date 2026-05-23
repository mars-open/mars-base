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
package net.mars.open.pp

import net.mars.open.pp.utils.GeometryCalcUtils._
import net.mars.open.pp.utils.SeqUtils.withPrevAndNext
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.locationtech.jts.geom.{Coordinate, CoordinateXYZM, Geometry, GeometryFactory}

import scala.collection.mutable

/**
 * Create position points at fixed intervals on edges. The position points are enriched with
 * additional information such as grade and azimuth, which are calculated based on the geometry of
 * the edge and its neighboring points.
 *
 * The position points are also enriched with a priority (prio) which is used when creating unique
 * position points and their mapping to edges. Lower priorities get snapped first to existing
 * position points. This is important for switches, as we want the "main" edge to be merged first,
 * so it has higher priority to create new position points in the region of the "Weichenzunge".
 *
 * The edge geometries must have coordinates in a metric CRS. Make sure to set the correct srcCrs
 * parameter when calling the transform method.
 */
class CreatePpTransformer extends CustomDfsTransformer {

  def transform(
      dsEdge: Dataset[Edge],
      dsNode: Dataset[Node],
      ppDistance: Float = 0.25f,
      wellDefinedPointDistance: Float = 25f,
      nbOfPartitions: Int = 100,
      srcCrs: String
  ): Dataset[RawPpWithMapping] = {
    val session = dsEdge.sparkSession
    import session.implicits._
    assert(isMetricCrs(srcCrs), "CRS must be metric for distance calculations; got " + srcCrs)

    val udfCreatePointsAtFixedInterval = udf(CreatePpTransformer.createPointsAtFixedInterval(ppDistance, wellDefinedPointDistance, srcCrs) _)
      .asNondeterministic()

    val dfNodes = dsNode
      .select($"uuid_node", $"edges")

    val dfPoints = dsEdge.as("edge")
      .join(dfNodes.as("nodeFrom"), $"edge.uuid_node_from" === $"nodeFrom.uuid_node")
      .withColumn("node_from_mapping", filter($"nodeFrom.edges", e => e("uuid_edge") === $"uuid_edge")(0))
      .join(dfNodes.as("nodeTo"), $"edge.uuid_node_to" === $"nodeTo.uuid_node")
      .withColumn("node_to_mapping", filter($"nodeTo.edges", e => e("uuid_edge") === $"uuid_edge")(0))
      .repartition(nbOfPartitions)
      .withColumn("pps", udfCreatePointsAtFixedInterval($"uuid_edge", $"geometry"))
      .withColumn("point", explode($"pps"))
      .withColumn("x", ST_X($"point.geometry"))
      .withColumn("y", ST_Y($"point.geometry"))
      .withColumn("z", when(!isnan(ST_Z($"point.geometry")), ST_Z($"point.geometry").cast("float")))
      .withColumn("position", ST_M($"point.geometry"))
      .as("pp")
      .withColumn("track", filter($"tracks", t => t("position_from") <= $"position" and  $"position" < t("position_to"))(0))
      .withColumn("tags", array_union($"tags", $"track.tags"))
      .withColumn("properties", map_concat($"properties", $"track.properties"))
      .withColumn("level", $"track.level")
      // TODO: can we somehow detect "Gleisdurchschneidung" and include in priorities?
      .withColumn("prio", when(
          ($"node_from_mapping.main_edge" and $"position".between($"node_from_mapping.position_from", $"node_from_mapping.position_to")) or
            ($"node_to_mapping.main_edge" and $"position".between($"node_to_mapping.position_from", $"node_to_mapping.position_to")),
          lit(1)
        ).otherwise(lit(2)).cast("short")
      )
      .select(
        $"point.*",
        $"x",
        $"y",
        $"z",
        $"level",
        $"uuid_edge",
        $"position",
        $"idx".as("edge_idx"),
        $"tags",
        $"properties",
        $"prio"
      )
      .drop("geometry", "idx")

    dfPoints.as[RawPpWithMapping]
  }

}
object CreatePpTransformer extends SmartDataLakeLogger {

  /**
   * Create points at given fixed interval. Note that we start counting from both side, creating a
   * potential gap which is somewhat larger than the interval in the middle of the edge.
   */
  def createPointsAtFixedInterval(interval: Double, wellDefinedPointDistance: Double, srcCrs: String)(uuid_edge: String, geom: Geometry): Seq[EdgePoint] = try {
    implicit val geoFactory: GeometryFactory = getGeoFactory(srcCrs)
    if (geom.getNumPoints >= 2) {
      val coords = enrichLinePosition(geom.getCoordinates.toSeq, uuid_edge)
      val length = coords.last.getM
      val linePoints = createLinePointsWithRadius(coords, wellDefinedPointDistance)
      val linePointsQueue = mutable.Queue(linePoints: _*)
      // calculate the number of points to create. If interval is bigger than interval / 2, there should be at least 1 point.
      val maxIdx = if (length > interval / 2) math.max(1, math.floor(length / interval).toInt)
      else 0 // this is the 1-based index of the last point, given by the length of the line geometry
      val remainingLength = length - maxIdx * interval
      val intervalPoints = (1 to maxIdx).map { idx =>
        // distribute remaining space equally at begin and end, start at interval/2 (idx - 0.5)
        val position = round5(remainingLength / 2 + (idx - 0.5) * interval)
        assert(position >= linePointsQueue(0).geometry.getM)
        while (position > linePointsQueue(1).geometry.getM) linePointsQueue.dequeue()
        assert(linePointsQueue.size >= 2)
        interpolatePoint(linePointsQueue(0), linePointsQueue(1), position, idx)
      }
      val globalAzimut = calcAzimuth(coords.head, coords.last)
      val edgePoints = createEdgePointsWithGradeAndAzimuth(intervalPoints, interval, globalAzimut)
      edgePoints
    } else {
      logger.error(s"Edge $uuid_edge has less than 2 points.")
      Seq()
    }
  } catch {
    case e: Throwable =>
      throw new RuntimeException(s"edge=$uuid_edge: ${e.getClass.getSimpleName}: ${e.getMessage}", e)
  }

  def interpolatePoint(p1: LinePoint, p2: LinePoint, position: Double, idx: Int): LinePoint = {
    val fraction = getFractionBetweenCoords(p1.geometry, p2.geometry, position)
    val coord = interpolateCoord(p1.geometry, p2.geometry, fraction, position)
    val radius = interpolateOptVal(p1.radius.map(_.toDouble), p2.radius.map(_.toDouble), fraction).map(_.toInt)
    LinePoint(coord, radius, p1.well_defined || p2.well_defined, Some(idx)) // if one of both is well-defined, it's sufficient
  }

  @inline
  def round5(v: Double): Double = math.round(v * 100000) / 100000d

  def getZoom(pos: Double, length: Double, interval: Double): Short = {
    val idx = math.floor(pos / interval).toInt
    if (pos == 0d || pos == length) 0
    else if (idx % (10 / interval) == 0) 1 // every 40th point (if interval=0.25)
    else if (idx % (1 / interval) == 0) 2 // every point 4th point (if interval=0.25)
    else 3
  }

  def createLinePointsWithRadius(coords: Seq[CoordinateXYZM], wellDefinedPointDistance: Double): Seq[LinePoint] =
    withPrevAndNext[CoordinateXYZM, LinePoint](coords) {
      case (prev, current, next) =>
        val wellDefined = Option(current.getZ).exists(_.isFinite) &&
          prev.forall(_.distance(current) <= wellDefinedPointDistance) &&
          next.forall(_.distance(current) <= wellDefinedPointDistance)
        val radius = ((prev, current, next) match {
          case (Some(a), b, Some(c)) if wellDefined => calcCircumRadius(a, b, c)
          case _                                    => None
        }).map(r => r.round.toInt)
        LinePoint(current, radius, wellDefined)
    }

  /**
   * Calculating Azimut on detailed points has the advantage to get the mean azimut at line points,
   * but between line points the azimuth to the next line point
   */
  def createEdgePointsWithGradeAndAzimuth(points: Seq[LinePoint], interval: Double, globalAzimuth: Double)(implicit
      geoFactory: GeometryFactory
  ): Seq[EdgePoint] =
    withPrevAndNext[LinePoint, EdgePoint](points) {
      case (prev, current, next) =>
        val grade = ((prev, current, next) match {
          case (Some(a), _, Some(b)) if a.geometry.getZ.isFinite && b.geometry.getZ.isFinite => calcGrade(a.geometry, b.geometry)
          case (Some(a), b, _) if a.geometry.getZ.isFinite && b.geometry.getZ.isFinite       => calcGrade(a.geometry, b.geometry)
          case (_, a, Some(b)) if a.geometry.getZ.isFinite && b.geometry.getZ.isFinite       => calcGrade(a.geometry, b.geometry)
          case _                                                                             => None
        }).map(_.toFloat)
        val azimuth = ((prev, current, next) match {
          case (Some(a), _, Some(b)) => calcAzimuth(a.geometry, b.geometry)
          case (Some(a), b, _)       => calcAzimuth(a.geometry, b.geometry)
          case (_, a, Some(b))       => calcAzimuth(a.geometry, b.geometry)
          case _                     => globalAzimuth
        }).toFloat
        EdgePoint(
          geoFactory.createPoint(current.geometry),
          getZoom(current.geometry.getM, points.last.geometry.getM, interval),
          current.radius,
          grade,
          azimuth,
          current.well_defined,
          current.idx.get
        )
    }
}

case class LinePoint(geometry: CoordinateXYZM, radius: Option[Int], well_defined: Boolean, idx: Option[Int] = None)

case class EdgePoint(geometry: Geometry, zoom: Short, radius: Option[Int], grade: Option[Float], azimuth: Float, well_defined: Boolean, idx: Int)

/**
 * @param x
 *   native coordinate 1
 * @param y
 *   native coordinate 2
 * @param z
 *   Height in m
 * @param level
 *   level of the track, e.g. for bridges or tunnels.
 * @param zoom
 *   the zoom level of this Positionpoint
 * @param tags
 *   additional information
 * @param uuid_edge
 *   edge that this positionpoint is created from
 * @param position
 *   position on edge of this positionpoint
 * @param edge_idx
 *   number of point on edge
 * @param prio
 *   priority when creating unique positionpoints and their mapping to edges. Lower priorities get
 *   snapped first to existing positionpoints. This is important for switches, as we want the "main"
 *   edge to be merged first, so it has higher priority to create new positionpoints in the region
 *   of the "Weichenzunge".
 */
case class RawPpWithMapping(
    x: Double,
    y: Double,
    z: Option[Float],
    level: Option[Short],
    zoom: Short,
    tags: Set[String],
    properties: Map[String, String],
    uuid_edge: String,
    position: Double,
    edge_idx: Int,
    prio: Short,
    radius: Option[Int],
    grade: Option[Float],
    azimuth: Float,
    well_defined: Boolean
) {
  @transient lazy val coordinate: Coordinate = new Coordinate(x, y, z.map(_.toDouble).getOrElse(Double.NaN))
  def getGeometry(implicit factory: GeometryFactory): Geometry = factory.createPoint(coordinate)
}
