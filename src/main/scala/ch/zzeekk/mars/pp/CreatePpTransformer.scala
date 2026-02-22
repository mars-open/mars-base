package ch.zzeekk.mars.pp

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils._
import ch.zzeekk.mars.pp.utils.SeqUtils.withPrevAndNext
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.locationtech.jts.geom.{Coordinate, CoordinateXYZM, Geometry, GeometryFactory}

import scala.collection.mutable

class CreatePpTransformer extends CustomDfsTransformer {

  def transform(
                 dsEdge: Dataset[Edge],
                 dsNode: Dataset[Node],
                 ppDistance: Float = 0.25f,
                 wellDefinedPointDistance: Float = 25f,
                 nbOfPartitions: Int = 25,
                 srcCrs: String
               ): Dataset[RawPpWithMapping] = {
    val session = dsEdge.sparkSession
    import session.implicits._

    val udfCreatePointsAtFixedInterval = udf(Tlm3dPpTransformer.createPointsAtFixedInterval(ppDistance, wellDefinedPointDistance, srcCrs) _)

    val dfNodes = dsNode
      .select($"uuid_node", $"edges")

    val dfPoints = dsEdge.as("edge")
      .join(dfNodes.as("nodeFrom"), $"edge.uuid_node_from"===$"nodeFrom.uuid_node")
      .withColumn("node_from_mapping", filter($"nodeFrom.edges", e => e("uuid_edge")===$"uuid_edge")(0))
      .join(dfNodes.as("nodeTo"), $"edge.uuid_node_to"===$"nodeTo.uuid_node")
      .withColumn("node_to_mapping", filter($"nodeTo.edges", e => e("uuid_edge")===$"uuid_edge")(0))
      .repartition(nbOfPartitions)
      .withColumn("pps", udfCreatePointsAtFixedInterval($"uuid_edge", $"geometry"))
      .withColumn("point", explode($"pps"))
      .withColumn("x", ST_X($"point.geometry"))
      .withColumn("y", ST_Y($"point.geometry"))
      .withColumn("z", ST_Z($"point.geometry").cast("float"))
      .withColumn("position", ST_M($"point.geometry"))
      // TODO: can we somehow detect "Gleisdurchschneidung" and include in priorities?
      .withColumn("prio", when(
        ($"node_from_mapping.main_edge" and $"position".between($"node_from_mapping.position_from", $"node_from_mapping.position_to")) or
          ($"node_to_mapping.main_edge" and $"position".between($"node_to_mapping.position_from", $"node_to_mapping.position_to")), lit(1)
      ).otherwise(lit(2)).cast("short"))
      .select(
        $"point.*",
        $"x", $"y", $"z",
        $"uuid_edge", $"position", $"idx".as("edge_idx"),
        $"tags",
        $"prio"
      )
      .drop("geometry", "idx")

      dfPoints.as[RawPpWithMapping]
  }

}
object Tlm3dPpTransformer extends SmartDataLakeLogger {

  /**
   * Create points at given fixed interval.
   * Note that we start counting from both side, creating a potential gap which is somewhat larger than the interval in the middle of the edge.
   */
  def createPointsAtFixedInterval(interval: Double, wellDefinedPointDistance: Double, srcCrs: String)(uuid_edge: String, geom: Geometry): Seq[EdgePoint] = try {
    implicit val geoFactory: GeometryFactory = getGeoFactory(srcCrs)
    if (geom.getNumPoints >= 2) {
      val coords = enrichLinePosition(geom.getCoordinates.toSeq, uuid_edge)
      val length = coords.last.getM
      val linePoints = createLinePointsWithRadius(coords, wellDefinedPointDistance)
      val linePointsQueue = mutable.Queue(linePoints:_*)
      val maxIdx = math.floor(length / interval).toInt // this is the 1-based index of the last point, given by the length of the line geometry
      val remainingLength = length - maxIdx * interval
      val intervalPoints = (1 to maxIdx).map { idx =>
        // distribute remaining space equally at begin and end, start at interval/2 (idx - 0.5
        val position = round5(remainingLength / 2 + (idx - 0.5) * interval)
        assert(position >= linePointsQueue(0).geometry.getM)
        while (position > linePointsQueue(1).geometry.getM) linePointsQueue.dequeue()
        assert(linePointsQueue.size >= 2)
        interpolatePoint(linePointsQueue(0), linePointsQueue(1), position, idx)
      }
      val edgePoints = createEdgePointsWithGradeAndAzimuth(intervalPoints, interval)
      edgePoints
    } else {
      logger.error(s"Edge $uuid_edge has less than 2 points.")
      Seq()
    }
  } catch {
    case e: Throwable =>
      throw new RuntimeException(s"edge=$uuid_edge: ${e.getClass.getSimpleName}: ${e.getMessage}")
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
    else if (idx % (10/interval) == 0) 1 // every 40th point (if interval=0.25)
    else if (idx % (1/interval) == 0) 2 // every point 4th point (if interval=0.25)
    else 3
  }

  def createLinePointsWithRadius(coords: Seq[CoordinateXYZM], wellDefinedPointDistance: Double): Seq[LinePoint] = {
    withPrevAndNext[CoordinateXYZM,LinePoint](coords) {
      case (prev, current, next) =>
        val wellDefined = Option(current.getZ).exists(_.isFinite) &&
          prev.forall(_.distance(current) <= wellDefinedPointDistance) &&
          next.forall(_.distance(current) <= wellDefinedPointDistance)
        val radius = ((prev, current, next) match {
          case (Some(a), b, Some(c)) if wellDefined => calcCircumRadius(a, b, c)
          case _ => None
        }).map(r => r.round.toInt)
        LinePoint(current, radius, wellDefined)
    }
  }

  /**
   * Calculating Azimut on detailed points has the advantage to get the mean azimut at line points,
   * but between line points the azimuth to the next line point
   */
  def createEdgePointsWithGradeAndAzimuth(points: Seq[LinePoint], interval: Double)(implicit geoFactory: GeometryFactory): Seq[EdgePoint] = {
    withPrevAndNext[LinePoint, EdgePoint](points) {
      case (prev,current,next) =>
        val grade = ((prev, current, next) match {
          case (Some(a), _, Some(b)) if a.geometry.getZ.isFinite && b.geometry.getZ.isFinite => calcGrade(a.geometry, b.geometry)
          case (Some(a), b, _) if a.geometry.getZ.isFinite && b.geometry.getZ.isFinite => calcGrade(a.geometry, b.geometry)
          case (_, a, Some(b)) if a.geometry.getZ.isFinite && b.geometry.getZ.isFinite => calcGrade(a.geometry, b.geometry)
          case _ => None
        }).map(_.toFloat)
        val azimuth = ((prev, current, next) match {
          case (Some(a), _, Some(b)) => calcAzimuth(a.geometry, b.geometry)
          case (Some(a), b, _) => calcAzimuth(a.geometry, b.geometry)
          case (_, a, Some(b)) => calcAzimuth(a.geometry, b.geometry)
        }).toFloat
        EdgePoint(geoFactory.createPoint(current.geometry),
          getZoom(current.geometry.getM, points.last.geometry.getM, interval),
          current.radius, grade, azimuth, current.well_defined, current.idx.get
        )
    }
  }
}

case class LinePoint(geometry: CoordinateXYZM, radius: Option[Int], well_defined: Boolean, idx: Option[Int] = None)

case class EdgePoint(geometry: Geometry, zoom: Short, radius: Option[Int], grade: Option[Float], azimuth: Float, well_defined: Boolean, idx: Int)

/**
 * @param x native coordinate 1
 * @param y native coordinate 2
 * @param z Height in m
 * @param zoom the zoom level of this Positionpoint
 * @param tags additional information
 * @param uuid_edge edge that this positionpoint is created from
 * @param position position on edge of this positionpoint
 * @param edge_idx number of point on edge
 * @param prio priority when creating unique positionpoints and their mapping to edges.
 *             Lower priorities get snapped first to existing positionpoints.
 *             This is important for switches, as we want the "main" edge to be merged first, so it has higher priority to create new positionpoints in the region of the "Weichenzunge".
 */
case class RawPpWithMapping(x: Double, y: Double, z: Float, zoom: Short, tags: Set[String], uuid_edge: String, position: Double, edge_idx: Int, prio: Short, radius: Option[Int], grade: Option[Float], azimuth: Float, well_defined: Boolean) {
  @transient lazy val coordinate: Coordinate = new Coordinate(x,y,z)
  def getGeometry(implicit factory: GeometryFactory): Geometry = factory.createPoint(coordinate)
}