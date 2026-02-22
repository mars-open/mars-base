package ch.zzeekk.mars.pp

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils.getGeoFactory
import ch.zzeekk.mars.pp.utils.SpatialIndex
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
 *
 * A priority can be given to Positionpoint candidates, to favor some candidates against others, e.g. the main branch of a switch against the turnout.
 * To avoid creating duplicates, the mapping and creation of new Positionpoints is done by priority and edge.
 *
 * To be correct at the boarder of cells, each priority and edge round has to include newly created points in neighbour cells as well.
 * The spatial join has therefore to be done multiple times.
 *
 * Note: H3 Cell viewer: https://clupasq.github.io/h3-viewer/
 */
class SnapPpTransformer extends CustomDfsTransformer {

  def transform(dsSlvPp: Dataset[Pp], dsSlvTlm3dPp: Dataset[PpWithMapping], ppRadius: Float = 0.25f, ppHeightTolerance: Float = 1f, srcCrs: String, isExec: Boolean): Map[String,DataFrame] = {
    implicit val session: SparkSession = dsSlvTlm3dPp.sparkSession
    import session.implicits._

    val udfH3idL15 = udf(PpIdGenerator.getH3idL15 _)

    // get list of prio values
    val prios = if (isExec) {
      dsSlvTlm3dPp
        .agg(collect_set($"prio").as("prios"))
        .as[Seq[Short]].head().sorted
    } else Seq(1)
    if (isExec) logger.info(s"prios=${prios.mkString(",")}")

    val dsExistingSnapped = dsSlvPp
      .withColumn("pp_existing", struct("*"))
      .select($"pp_existing".as("snapped_pp"), lit(false).as("is_new_pp"), typedlit[PpWithMapping](null).as("pp"), lit(null).as("direction"))
      .as[PpWithMappingAndSnap]

    val dsNewSnapped = prios.foldLeft(dsExistingSnapped) {
      case (dsExistingSnapped, prio) =>

        val dfNewFiltered =  dsSlvTlm3dPp
          .where($"prio" === prio)
          .withColumn("pp_new", struct("*"))
          .withColumn("h3id", udfH3idL15($"x", $"y", lit(srcCrs)))
          .groupBy($"h3id")
          .agg(collect_list($"pp_new").as("pps_new"))

        // TODO: cogroup in snapIteration doesnt work in Scala 2.13 / Spark 3.x
        //val dsNewFiltered = dfNewFiltered
        //  .select($"h3id", $"pp_new")
        //  .as[(Long, PpWithMapping)]
        //val dsNewSnapped = snapIteration(
        // dsNewFiltered,
        // dsExistingSnapped.where($"is_new_pp" || $"pp".isNull),
        // ppRadius, ppHeightTolerance, srcCrs)
        //dfNewFiltered.cache.show
        //dsNewSnapped.cache.select($"pp.*", $"is_new_pp").show

        val dfExisting = dsExistingSnapped
          .withColumn("pp", struct("snapped_pp.*"))
          .withColumn("h3id_neighbours", ST_H3KRing($"snapped_pp.h3id", 1, exactRing = false)) // exact=false will include cell 0-kth neighbours, true only kth neighbours
          .withColumn("h3id", explode($"h3id_neighbours"))
          .groupBy($"h3id")
          .agg(collect_list($"pp").as("pps_existing"))

        val udfSnapNewToExistingPps = udf(snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs) _)
        val dsNewSnapped = dfNewFiltered
          .join(dfExisting, Seq("h3id"), "left")
          .withColumn("pp_snapped", explode(udfSnapNewToExistingPps($"pps_new", $"pps_existing", $"h3id")))
          .select($"pp_snapped.*")
          .as[PpWithMappingAndSnap]

        dsExistingSnapped.unionByName(dsNewSnapped)
    }
      .cache()

    val dsPpAppend = dsNewSnapped
      .filter(_.is_new_pp)
      .map(_.snapped_pp)

    val dfPpTlm3dOverwrite = dsNewSnapped
      .where($"pp".isNotNull) // remove initial existing pps
      .select(
        $"snapped_pp.id_positionpoint", $"direction", $"snapped_pp.zoom",
        $"pp.uuid_edge", $"pp.position", // TODO: adapt position when snapped to existing pp
        $"pp.edge_idx"
      )

    Map(
      "slv-pp" -> dsPpAppend.toDF(),
      "slv-pp-mapping" -> dfPpTlm3dOverwrite,
      "pp-mapping"-> dsNewSnapped.toDF()
    )
  }

  // TODO: doesnt work with Spark 3.5.x and Scala 2.13. Cogroup returns wrong results, see also https://issues.apache.org/jira/browse/SPARK-47061
  def snapIteration(dsNewH3: Dataset[(Long, PpWithMapping)], dsExisting: Dataset[Pp], ppRadius: Float, ppHeightTolerance: Float, srcCrs: String)(implicit session: SparkSession): Dataset[PpWithMappingAndSnap] ={
    import session.implicits._

    // explode existing points for all neighbours
    val dsExistingH3WithNeighbours = dsExisting
      .withColumn("pp", struct("*"))
      .withColumn("h3id_neighbours", ST_H3KRing($"snapped_pp.h3id", 1, exactRing = false)) // exact=false will include cell 0-kth neighbours, true only kth neighbours
      .withColumn("h3id", explode($"h3id_neighbours"))
      .select($"h3id", $"pp.snapped_pp")
      .as[(Long, Pp)]
      .groupByKey(_._1).mapValues(_._2)

    val dsNewSnapped = dsNewH3
      .groupByKey(_._1).mapValues(_._2)
      .cogroup(dsExistingH3WithNeighbours){
        case (_, newPps, _) if newPps.isEmpty => Seq()
        case (h3id, newPps, existingPps) =>
          val newPpsSeq = newPps.toSeq
          logger.info(s"h3id=$h3id ${newPpsSeq.map(_.edge_idx).mkString(",")}")
          val snappedPps = snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(newPpsSeq, existingPps.toSeq, h3id)
          snappedPps
      }

    dsNewSnapped
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
    if (newPps.isEmpty) return Seq()
    implicit val geoFactory: GeometryFactory = getGeoFactory(srcCrs)

    // build index of existing pps
    val index = new SpatialIndex(existingPps, (pp: Pp) => pp.getGeometry)

    // initialize id generator
    val cntPpsInTargetH3id = index.count(_.h3id == h3id)
    val ppIdGenerator = new PpIdGenerator(cntPpsInTargetH3id, h3id)

    // lookup existing pp within xy=25cm/z=1m for every new pp
    // process new pp mappings grouped by edges to minimize potential for creating duplicate pp.
    val snappedPps = newPps.groupBy(pp => pp.uuid_edge).toSeq.sortBy(_._1).flatMap {
      case (uuid_edge, pps) =>
        if (logger.isDebugEnabled) logger.debug(s"mapping ${pps.size} Positionpoints for edge $uuid_edge ($h3id)")
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
            PpWithMappingAndSnap(Some(ppMapping), pp, direction, is_new_pp = ppsInRange.isEmpty)
        }
        index.insert(snappedPps.filter(_.is_new_pp).map(_.snapped_pp)) // add created pps to index for next priority group
        snappedPps
    }
    snappedPps
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
case class Pp(id_positionpoint: Long, zoom: Short, h3id: Long, x: Double, y: Double, z: Float, lng: Double, lat: Double, azimuth: Float, token: String) {
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

/**
 * @param x native coordinate 1
 * @param y native coordinate 2
 * @param z Height in m
 * @param zoom the zoom level of this Positionpoint
 * @param uuid_edge edge that this positionpoint is created from
 * @param position position on edge of this positionpoint
 * @param edge_idx number of point on edge
 * @param prio priority when creating unique positionpoints and their mapping to edges.
 *             Lower priorities get snapped first to existing positionpoints.
 *             This is important for switches, as we want the "main" edge to be merged first, so it has higher priority to create new positionpoints in the region of the "Weichenzunge".
 */
case class PpWithMapping(x: Double, y: Double, z: Float, zoom: Short, position: Double, uuid_edge: String, prio: Short, azimuth: Float, edge_idx: Int = 0) {
  @transient lazy val coordinate: Coordinate = new Coordinate(x,y,z)
  def getGeometry(implicit factory: GeometryFactory): Geometry = factory.createPoint(coordinate)
}

case class PpWithMappingAndSnap(pp: Option[PpWithMapping], snapped_pp: Pp, direction: Option[Short], is_new_pp: Boolean)