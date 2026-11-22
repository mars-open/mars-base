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

import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import net.mars.open.pp.utils.GeometryCalcUtils._
import net.mars.open.pp.utils.SpatialIndex
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_constructors._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.ST_Contains
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.json4s.{DefaultFormats, Extraction}
import org.locationtech.jts.algorithm.Angle
import org.locationtech.jts.densify.Densifier
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}

/**
 * Lookup and snap Positionpoint candidates created from edges to existing Positionpoints. The
 * spatial join is organized by H3 cells. To be correct at the border of cells, existing
 * Positionpoints from neighbor cells need to be included in the search.
 *
 * A priority can be given to Positionpoint candidates, to favor some candidates against others,
 * e.g. the main branch of a switch against the turnout. To avoid creating duplicates, the mapping
 * and creation of new Positionpoints is done by priority and edge.
 *
 * To be correct at the boarder of cells, each priority and edge round has to include newly created
 * points in neighbor cells as well. The spatial join has therefore to be done multiple times.
 *
 * Note: H3 Cell viewer: https://clupasq.github.io/h3-viewer/ or https://h3geo.org/
 */
class SnapPpTransformer extends CustomDfsTransformer {

  /**
   * @param dsPpExisting
   *   Dataset of existing Positionpoints. This is used to lookup existing points to snap to and to
   *   avoid creating duplicates. It contains coordinates in srcCrs (x,y) and EPSG:4326 (lat,lng).
   * @param dsPpNew
   *   Dataset of Positionpoint candidates with their mapping to edges. The coordinates are in the
   *   same CRS as srcCrs.
   * @param ppRadius
   * @param ppHeightTolerance
   * @param nbOfSparkPartitions
   * @param srcCrs
   * @param polygonCoordsFilter
   *   Define a filter to limit the processed region. The Polygon must be defined as list of json
   *   coordinate tuples. Use https://boundingbox.klokantech.com/ to select a region. Coordinates
   *   can be EPSG:4326 or in the same CRS as the input data (srcCrs).
   * @return
   */
  def transform(
                 dsPpExisting: Dataset[Pp],
                 dsPpNew: Dataset[PpWithMapping],
                 ppRadius: Float = 0.25f,
                 ppHeightTolerance: Float = 1f,
                 nbOfSparkPartitions: Int = 100,
                 srcCrs: String,
                 polygonCoordsFilter: Option[String] = None,
                 isExec: Boolean
  ): Map[String, DataFrame] = {
    implicit val session: SparkSession = dsPpExisting.sparkSession
    import org.json4s.jackson.JsonMethods._
    import session.implicits._
    assert(isMetricCrs(srcCrs), "CRS must be metric for distance calculations; got " + srcCrs)

    val srcRegionPolygon = polygonCoordsFilter.map { geoJson =>
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
      val coordinates = Extraction.extract[Seq[Seq[Double]]](parse(geoJson))
        .map(c => new Coordinate(c.head, c.last))
      val polygon = getGeoFactory(srcCrs).createPolygon(coordinates.toArray)
      // check CRS, and convert from EPSG:4326 to srcCrs if needed.
      if (isValidWithinCrs("EPSG:4326", polygon)) convertFrom4326(polygon, srcCrs)
      else if (isValidWithinCrs(srcCrs, polygon)) polygon
      else throw new IllegalArgumentException(s"Invalid filter polygon: Coordinates must be valid in CRS $srcCrs or EPSG:4326: " + geoJson)
    }
    def ST_Polygon(polygon: Geometry): Column =
      ST_MakePolygon(ST_MakeLine(array(polygon.getCoordinates.map(coord => ST_Point(lit(coord.x), lit(coord.y))): _*)))

    val dsPpNewFiltered = srcRegionPolygon.map(polygon => dsPpNew.filter(ST_Contains(ST_Polygon(polygon), ST_Point($"x", $"y"))))
      .getOrElse(dsPpNew)

    // Candidates of all prios are keyed by their h3 cell and partitioned only once.
    // Filtering a prio keeps the hash partitioning, so the rounds don't need to shuffle candidates again.
    val dfNewKeyed = keyNewPps(dsPpNewFiltered, srcCrs, nbOfSparkPartitions)
      .persist()

    // get list of prio values and the bounding box of the candidates.
    // Read them from the input and not from dfNewKeyed, as only the prio and coordinate columns need to be read.
    val (prios, newPpsEnvelope) = if (isExec) {
      val stats = dsPpNewFiltered
        .agg(collect_set($"prio").as("prios"), min($"x"), max($"x"), min($"y"), max($"y"))
        .as[(Seq[Short], Option[Double], Option[Double], Option[Double], Option[Double])]
        .head()
      val envelope = for (minX <- stats._2; maxX <- stats._3; minY <- stats._4; maxY <- stats._5) yield new Envelope(minX, maxX, minY, maxY)
      (stats._1.sorted, envelope)
    } else (Seq(1.toShort), None)
    if (isExec) logger.info(s"Found prios ${prios.mkString(",")}")
    assert(prios.nonEmpty, "No prio values found in input data, input count is " + dfNewKeyed.count())

    // Only existing pps around the candidates are needed. Filter them by the bounding box of the candidates with plain comparisons,
    // so that Delta can skip files by their min/max statistics. This also covers the filter polygon, as candidates are filtered by it.
    val dsPpExistingFiltered = newPpsEnvelope
      .map { envelope =>
        val bbox = getEnvelope4326WithBuffer(envelope, ppRadius * 2, srcCrs)
        logger.info(s"Filtering existing pps by bounding box of candidates $bbox")
        dsPpExisting.where($"lng".between(bbox.getMinX, bbox.getMaxX) && $"lat".between(bbox.getMinY, bbox.getMaxY))
      }
      .getOrElse(dsPpExisting)

    // Existing pps are keyed by their h3 cell and neighbour cells only once and persisted, so that later rounds
    // neither read and explode nor shuffle them again. Later rounds only add the keyed pps created by previous rounds.
    // Persist is used instead of a checkpoint, as a checkpoint loses the hash partitioning.
    val dfExistingKeyed = keyExistingPps(dsPpExistingFiltered, ppRadius, nbOfSparkPartitions)
      .persist()

    // snap one priority after the other, each round sees the pps created by the previous rounds
    val (_, dsSnappedRounds) = prios.foldLeft((dfExistingKeyed, Seq[Dataset[PpWithMappingAndSnap]]())) {
      case ((dfExistingKeyed, dsSnappedRounds), prio) =>
        // Each round sees the pps created by previous rounds in its own and neighbour cells, as they are added to dfExistingKeyed
        // with the same neighbour keys as existing pps.
        val dfNewPrio = dfNewKeyed.where($"prio" === prio)
        // The result of this round is used twice: for the outputs and as input for the next round.
        // Use a lazy local checkpoint instead of persist as it is written to slv-pp, which is also an input and invalidates the cached DataFrame for the second output.
        val dsSnapped = snapIteration(dfNewPrio, dfExistingKeyed, ppRadius, ppHeightTolerance, srcCrs)
          .localCheckpoint(eager = false)
        val dsCreatedPps = dsSnapped.where($"is_new_pp").select($"snapped_pp.*").as[Pp]
        (dfExistingKeyed.unionByName(keyExistingPps(dsCreatedPps, ppRadius, nbOfSparkPartitions)), dsSnappedRounds :+ dsSnapped)
    }
    // outputs only need the results of the rounds, existing pps are not repeated
    val dsNewSnapped = dsSnappedRounds.reduce(_ unionByName _)

    val dsPpAppend = dsNewSnapped
      .filter(_.is_new_pp)
      .map(_.snapped_pp)

    val dfPpMappingOverwrite = dsNewSnapped
      .select(
        $"snapped_pp.id_positionpoint",
        $"direction",
        $"snapped_pp.zoom",
        $"pp.uuid_edge",
        $"position",
        $"pp.edge_idx"
      )

    Map(
      "slv-pp"         -> dsPpAppend.toDF(),
      "slv-pp-mapping" -> dfPpMappingOverwrite,
      "pp-mapping"     -> dsNewSnapped.toDF()
    )
  }

  /**
   * Position of an existing Positionpoint on the edge of a candidate: the candidate position plus the offset to the existing
   * Positionpoint projected on the edge direction. The edge direction is given by the candidate azimuth, see [[calcAzimuth]].
   * The position is not negative, as an existing Positionpoint before the start of the edge is mapped to the start.
   */
  def getPositionOnEdge(candidate: PpWithMapping, existing: Pp): Double = {
    val offset = (existing.x - candidate.x) * Math.cos(candidate.azimuth) + (existing.y - candidate.y) * Math.sin(candidate.azimuth)
    Math.max(0d, candidate.position + offset)
  }

  /**
   * Convert an envelope in srcCrs with a buffer to an envelope in EPSG:4326.
   * The boundary is densified before converting, so that the curvature of the transformed boundary is covered.
   */
  def getEnvelope4326WithBuffer(envelope: Envelope, buffer: Double, srcCrs: String): Envelope = {
    val bufferedEnvelope = new Envelope(envelope)
    bufferedEnvelope.expandBy(buffer)
    val polygon = getGeoFactory(srcCrs).toGeometry(bufferedEnvelope)
    convertTo4326(Densifier.densify(polygon, 1000), srcCrs).getEnvelopeInternal
  }

  // cogroup needs identical key types incl. nullability for new and existing pps.
  private def nonNullableKey(key: Column): Column = coalesce(key, lit(-1L))

  /**
   * Key existing Positionpoints by their h3 cell and the neighbour cells within ppRadius, as only candidates in these cells can
   * snap to them. The result is hash partitioned by the key, so that it can be unioned with further keyed Positionpoints and
   * cogrouped with candidates without shuffling it again.
   */
  def keyExistingPps(dsExisting: Dataset[Pp], ppRadius: Float, nbOfSparkPartitions: Int): DataFrame = {
    import dsExisting.sparkSession.implicits._
    val udfH3idWithNeighbours = udf((lat: Double, lng: Double, h3id: Long) => PpIdGenerator.getH3idL15WithNeighboursInRange(lat, lng, h3id, ppRadius))
    dsExisting
      .withColumn("h3id_key", explode(udfH3idWithNeighbours($"lat", $"lng", $"h3id")))
      .withColumn("h3id_key", nonNullableKey($"h3id_key"))
      .repartition(nbOfSparkPartitions, $"h3id_key")
  }

  /**
   * Key Positionpoint candidates by their h3 cell (resolution 15). The result is hash partitioned the same way as
   * [[keyExistingPps]], so that both can be cogrouped without shuffling.
   */
  def keyNewPps(dsNew: Dataset[PpWithMapping], srcCrs: String, nbOfSparkPartitions: Int): DataFrame = {
    import dsNew.sparkSession.implicits._
    val udfH3idL15 = udf(PpIdGenerator.getH3idL15 _)
    dsNew
      .withColumn("h3id_key", nonNullableKey(udfH3idL15($"x", $"y", lit(srcCrs))))
      .repartition(nbOfSparkPartitions, $"h3id_key")
  }

  /**
   * Snap Positionpoint candidates to existing Positionpoints, grouped by h3 cell (resolution 15).
   * @param dfNewKeyed
   *   Positionpoint candidates keyed and partitioned by [[keyNewPps]].
   * @param dfExistingKeyed
   *   existing Positionpoints keyed and partitioned by [[keyExistingPps]].
   */
  def snapIteration(
      dfNewKeyed: DataFrame,
      dfExistingKeyed: DataFrame,
      ppRadius: Float,
      ppHeightTolerance: Float,
      srcCrs: String
  )(implicit session: SparkSession): Dataset[PpWithMappingAndSnap] = {
    import session.implicits._
    val kvNew = dfNewKeyed.groupBy($"h3id_key").as[Long, PpWithMapping]

    val kvExisting = dfExistingKeyed.groupBy($"h3id_key").as[Long, Pp]

    kvNew.cogroup(kvExisting) {
      case (_, newPps, _) if newPps.isEmpty => Seq.empty[PpWithMappingAndSnap]
      case (h3id, newPps, existingPps)      => snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(newPps.toSeq, existingPps.toSeq, h3id)
    }
  }

  /**
   * @param radius
   *   The radius that a Positionpoint covers of its surroundings
   * @param heightTolerance
   *   The tolerance in height before creating a new Positionpoint
   * @param newPps
   *   New Positionpoints with their mapping to a topology. All new positionpoints should belong to
   *   a given h3 cell
   * @param existingPps
   *   Existing Positionpoints in the given h3 cell and their direct neighbours.
   * @param h3id
   *   The id of the h3 cell that newPps belong to.
   */
  def snapNewToExistingPps(
      radius: Float,
      heightTolerance: Float,
      srcCrs: String
  )(newPps: Seq[PpWithMapping], existingPps: Seq[Pp], h3id: Long): Seq[PpWithMappingAndSnap] = {
    if (newPps.isEmpty) return Seq()
    implicit val geoFactory: GeometryFactory = getGeoFactory(srcCrs)
    assert(isMetricCrs(srcCrs), "CRS must be metric for distance calculations; got " + srcCrs)

    // build index of existing pps
    val index = new SpatialIndex(existingPps, (pp: Pp) => pp.getGeometry)

    // initialize id generator: continue after the highest number used in the target cell, so that ids of existing pps are never reused.
    val ppIdGenerator = new PpIdGenerator(PpIdGenerator.nextPpNumber(existingPps.filter(_.h3id == h3id).map(_.id_positionpoint)), h3id)

    // lookup existing pp within xy=25cm/z=1m for every new pp
    // process new pp mappings grouped by edges to minimize potential for creating duplicate pp.
    val snappedPps = newPps.groupBy(pp => pp.uuid_edge).toSeq.sortBy(_._1).flatMap {
      case (uuid_edge, pps) =>
        if (logger.isDebugEnabled) logger.debug(s"mapping ${pps.size} Positionpoints for edge $uuid_edge ($h3id)")
        val snappedPps = pps.map {
          ppMapping =>
            // lookup existing pp in range
            val ppsInRange = index.queryInRangeOf(ppMapping.getGeometry, radius) // this filters x/y
              .filter { pp =>
                (pp.z, ppMapping.z, pp.level, ppMapping.level) match {
                  case (Some(ppZ), Some(ppmZ), _, _)         => Math.abs(ppZ - ppmZ) <= heightTolerance
                  case (_, _, Some(ppLevel), Some(ppmLevel)) => ppLevel == ppmLevel
                  case _                                     => true // if no height or level information is available, ignore height for snapping
                }
              }
            // create new if pp not found
            val pp = if (ppsInRange.isEmpty) {
              val ppCreated = Pp.from(ppMapping, h3id, ppIdGenerator.nextPpId, srcCrs)
              if (logger.isDebugEnabled) logger.debug(s"created Positionpoint ${ppCreated.id_positionpoint} ($h3id)")
              ppCreated
            } else ppsInRange.head
            // calculate mapping direction
            val azimuthDiff = Angle.diff(ppMapping.azimuth, pp.azimuth)
            val direction = if (azimuthDiff < 45) Some(1.toShort) else if (azimuthDiff > 180 - 45) Some(-1.toShort) else Option.empty[Short]
            // the position of the existing pp on the edge differs from the candidate position by the offset along the edge
            val position = if (ppsInRange.isEmpty) ppMapping.position else getPositionOnEdge(ppMapping, pp)
            PpWithMappingAndSnap(Some(ppMapping), pp, direction, is_new_pp = ppsInRange.isEmpty, position)
        }
        index.insert(snappedPps.filter(_.is_new_pp).map(_.snapped_pp)) // add created pps to index for next priority group
        snappedPps
    }
    snappedPps
  }
}

/**
 * @param id_positionpoint
 *   64bit unique id of the Positionpoint
 * @param x
 *   native coordinate 1
 * @param y
 *   native coordinate 2
 * @param z
 *   Height in m
 * @param level
 *   level of the track, e.g. for bridges or tunnels.
 * @param zoom
 *   Zoom level of the Positionpoint 0: point from node 1: additional 10m points 2: additional 1m
 *   points 3: additional 0.25cm points
 * @param h3id
 *   Id of the h3 cell
 * @param lat
 *   GPS coordinate latitude
 * @param lng
 *   GPS coordinate longitude
 * @param azimuth
 *   Direction of edge that triggered creation of this Positionpoint. This is used to manage
 *   direction sensitive values.
 * @param token
 *   Base32 unique token of the Positionpoint in format XXXX-XXXX-XXXXX. This is a human friendly
 *   representation of the id.
 */
case class Pp(
    id_positionpoint: Long,
    zoom: Short,
    h3id: Long,
    x: Double,
    y: Double,
    z: Option[Float],
    level: Option[Short],
    lng: Double,
    lat: Double,
    azimuth: Float,
    token: String
) {
  @transient lazy val coordinate: Coordinate = new Coordinate(x, y, z.map(_.toDouble).getOrElse(Double.NaN))
  def getGeometry(implicit factory: GeometryFactory): Geometry = factory.createPoint(coordinate)
}

object Pp {
  def from(ppm: PpWithMapping, h3id: Long, id: Long, srcCrs: String): Pp = {
    implicit val geoFactory: GeometryFactory = getGeoFactory(srcCrs)
    val wgs84coord = if (srcCrs != "EPSG:4326") convertTo4326(ppm.getGeometry, srcCrs).getCoordinate
    else ppm.coordinate
    Pp(id, ppm.zoom, h3id, ppm.x, ppm.y, ppm.z, ppm.level, lng = wgs84coord.getX, lat = wgs84coord.getY, azimuth = ppm.azimuth,
      token = PpIdGenerator.getToken(id))
  }
}

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
case class PpWithMapping(
    x: Double,
    y: Double,
    z: Option[Float],
    level: Option[Short],
    zoom: Short,
    position: Double,
    uuid_edge: String,
    prio: Short,
    azimuth: Float,
    edge_idx: Int = 0
) {
  @transient lazy val coordinate: Coordinate = new Coordinate(x, y, z.map(_.toDouble).getOrElse(Double.NaN))
  def getGeometry(implicit factory: GeometryFactory): Geometry = factory.createPoint(coordinate)
}

/**
 * @param pp
 *   Positionpoint candidate
 * @param snapped_pp
 *   existing or newly created Positionpoint the candidate is mapped to
 * @param position
 *   position of snapped_pp on the edge of the candidate. If snapped to an existing Positionpoint, this is the candidate position
 *   adapted by the offset of the existing Positionpoint along the edge.
 */
case class PpWithMappingAndSnap(pp: Option[PpWithMapping], snapped_pp: Pp, direction: Option[Short], is_new_pp: Boolean, position: Double)
