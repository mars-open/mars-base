package ch.zzeekk.mars.pp

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils.getGeoFactory
import ch.zzeekk.mars.pp.utils.SpatialIndex
import ch.zzeekk.mars.tlm3d.PpWithMapping
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.sedona.common.FunctionsGeoTools
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.locationtech.jts.algorithm.Angle
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

/**
 * Lookup and snap Positionpoint candidates created from edges to existing Positionpoints.
 * The spatial join is organized by H3 cells.
 * To be correct at the border of cells, existing Positionpoints from neighbour cells need to be included in the search.
 * A priority can be given to Positionpoint candidates, to favor some candidates against others, e.g. the main branch of a switch against the turnout.
 * To avoid creating duplicates, the mapping and creation of new Positionpoints is done by edge and priority.
 * Note: H3 Cell viewer: https://clupasq.github.io/h3-viewer/
 */
class SnapPpTransformer extends CustomDfsTransformer {

  def transform(dsSlvPp: Dataset[Pp], dsSlvTlm3dPp: Dataset[PpWithMapping], ppRadius: Float = 0.25f, ppHeightTolerance: Float = 1f, srcCrs: String): Map[String,DataFrame] = {
    implicit val session: SparkSession = dsSlvTlm3dPp.sparkSession
    import session.implicits._

    val udfH3idL15 = udf(PpIdGenerator.getH3idL15 _)

    val dfNewH3 = dsSlvTlm3dPp
      .withColumn("h3id", udfH3idL15($"x", $"y", lit(srcCrs)))
      .withColumn("pp_new", struct("*"))
      .groupBy($"h3id")
      .agg(collect_list($"pp_new").as("pps_new"))
      .withColumn("h3id_neighbours", ST_H3KRing($"h3id", 1, exactRing = false)) // exact=false will include cell 0-kth neighbours, true only kth neighbours

    val dfExistingH3WithNeighbours = dsSlvPp
      .withColumn("pp_existing", struct("*"))
      // explode existing points for all neighbours
      .withColumn("h3id_neighbours", ST_H3KRing($"h3id", 1, exactRing = false))
      // exact=false will include cell 0-kth neighbours, true only kth neighbours
      .withColumn("h3id", explode($"h3id_neighbours"))
      .groupBy($"h3id")
      .agg(collect_list($"pp_existing").as("pps_existing"))

    val udfSnapNewToExistingPps = udf(snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs) _)
    val dsNewSnapped = dfNewH3
      .join(dfExistingH3WithNeighbours, Seq("h3id"), "left")
      .withColumn("pps_new_snapped", udfSnapNewToExistingPps($"pps_new", coalesce($"pps_existing",array()), $"h3id"))
      .withColumn("pp_snapped", explode($"pps_new_snapped"))
      .select($"pp_snapped.*")
      .as[PpWithMappingAndSnap]
      .cache()

    val dsPpAppend = dsNewSnapped
      .filter(_.is_new_pp)
      .map(_.snapped_pp)

    val dfPpTlm3dOverwrite = dsNewSnapped
      .select(
        $"snapped_pp.id_positionpoint", $"direction", $"snapped_pp.zoom",
        $"pp.uuid_edge", $"pp.position", // TODO: adapt position when snapped to existing pp
        $"pp.tags", $"pp.radius", $"pp.grade", $"pp.azimuth", $"pp.well_defined"
      )

    Map(
      "slv-pp" -> dsPpAppend.toDF,
      "slv-pp-tlm3d" -> dfPpTlm3dOverwrite,
      "pp-mapping"-> dsNewSnapped.toDF
    )
  }

  /**
   * @param radius The radius that a Positionpoint covers of its surroundings
   * @param heightTolerance The tolerance in height before creating a new Positionpoint
   * @param newPps New Positionpoints with their mapping to a topology.
   *               All new positionpoints should belong to a given h3 cell
   * @param existingPps Existing Positionpoints in the given h3 cell and their direct neighbours.
   * @param h3id The id of the h3 cell that newPps belong to.
   */
  def snapNewToExistingPps(radius: Float, heightTolerance: Float, srcCrs: String)(newPps: Seq[PpWithMapping], existingPps: Seq[Pp], h3id: Long): Seq[PpWithMappingAndSnap] = {
    implicit val geoFactory: GeometryFactory = getGeoFactory(srcCrs)
    val cntPpsInTargetH3id = existingPps.count(_.h3id == h3id).toShort
    val ppIdGenerator = new PpIdGenerator(cntPpsInTargetH3id, h3id)

    // build index of existing pps
    val index = new SpatialIndex(existingPps, (pp: Pp) => pp.getGeometry)

    // lookup existing pp within xy=25cm/z=1m for every new pp
    // process pps grouped by priority, starting with lowest prio.
    newPps.groupBy(pp => (pp.prio, pp.uuid_edge)).toSeq.sortBy(_._1).flatMap {
      case ((prio, uuid_edge), pps) =>
        if (logger.isDebugEnabled) logger.debug(s"mapping ${pps.size} Positionpoints with prio $prio for edge $uuid_edge ($h3id)")
        val snappedPps = pps.map {
          ppMapping =>
            val geometry = ppMapping.getGeometry
            // lookup existing pp in range
            val ppsInRange = index.queryInRangeOf(geometry, radius) // this filters x/y
              .filter(pp => Math.abs(pp.z - ppMapping.z) <= heightTolerance) // and then filter height
            // create new if pp not found
            val pp = if (ppsInRange.isEmpty) {
              val ppCreated = Pp.from(ppMapping, h3id, ppIdGenerator.nextPpId, srcCrs)
              if (logger.isDebugEnabled) logger.debug(s"created Positionpoint ${ppCreated.id_positionpoint} ($h3id)")
              ppCreated
            } else ppsInRange.head
            // calculate mapping direction
            val azimuthDiff = Angle.diff(ppMapping.azimuth, pp.azimuth)
            val direction = if (azimuthDiff < 45) Some(1.toShort) else if (azimuthDiff > 180 - 45) Some(-1.toShort) else None
            PpWithMappingAndSnap(ppMapping, pp, direction, is_new_pp = ppsInRange.isEmpty)
        }
        index.insert(snappedPps.filter(_.is_new_pp).map(_.snapped_pp)) // add created pps to index for next priority group
        snappedPps
    }
  }
}

/**
 * @param id_positionpoint 64bit unique id of the Positionpoint
 * @param x native coordinate 1
 * @param y native coordinate 2
 * @param z Height in m
 * @param zoom Zoom level of the Positionpoint
 *             0: point from node
 *             1: additional 10m points
 *             2: additional 1m points
 *             3: additional 0.25cm points
 * @param h3id Id of the h3 cell
 * @param lat GPS coordinate latitude
 * @param lng GPS coordinate longitude
 * @param azimuth Direction of edge that triggered creation of this Positionpoint.
 *                This is used to manage direction sensitive values.
 * @param token Base32 unique token of the Positionpoint in format XXXX-XXXX-XXXXX.
 *              This is a human friendly representation of the id.
 */
case class Pp(id_positionpoint: Long, zoom: Short, h3id: Long, x: Double, y: Double, z: Float, lat: Double, lng: Double, azimuth: Float, token: String) {
  @transient lazy val coordinate: Coordinate = new Coordinate(x,y,z)
  def getGeometry(implicit factory: GeometryFactory): Geometry = factory.createPoint(coordinate)
}

object Pp {

  def from(ppm: PpWithMapping, h3id: Long, id: Long, srcCrs: String): Pp = {
    implicit val geoFactory: GeometryFactory = getGeoFactory(srcCrs)
    val wgs84coord = FunctionsGeoTools.transform(ppm.getGeometry, srcCrs, "EPSG:4326").getCoordinate
    Pp(id, ppm.zoom, h3id, ppm.x, ppm.y, ppm.z, wgs84coord.getX, wgs84coord.getY, ppm.azimuth, PpIdGenerator.getToken(id))
  }

}

case class PpWithMappingAndSnap(pp: PpWithMapping, snapped_pp: Pp, direction: Option[Short], is_new_pp: Boolean)