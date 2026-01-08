package ch.zzeekk.mars.tlm3d

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils._
import ch.zzeekk.mars.pp.utils.SeqUtils.withPrevAndNext
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.locationtech.jts.geom.{Coordinate, CoordinateXYZM, Geometry, GeometryFactory}

import scala.collection.mutable

class Tlm3dPpTransformer extends CustomDfsTransformer {

  def transform(
                 dsSlvTlm3dEdge: Dataset[Edge],
                 dsSlvTlm3dNode: Dataset[Node],
                 ppDistance: Float = 0.25f,
                 wellDefinedPointDistance: Float = 25f,
                 nbOfPartitions: Int = 25,
                 srcCrs: String
               ): Dataset[PpWithMapping] = {
    val session = dsSlvTlm3dEdge.sparkSession
    import session.implicits._

    val udfCreatePointsAtFixedInterval = udf(Tlm3dPpTransformer.createPointsAtFixedInterval(ppDistance, wellDefinedPointDistance, srcCrs) _)

    val dfNodes = dsSlvTlm3dNode
      .select($"uuid_node", $"edges")

    val dfPoints = dsSlvTlm3dEdge.as("edge")
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
      .withColumn("prio", when(
        ($"node_from_mapping.main_edge" and $"position".between($"node_from_mapping.position_from", $"node_from_mapping.position_to")) or
          ($"node_to_mapping.main_edge" and $"position".between($"node_to_mapping.position_from", $"node_to_mapping.position_to")), lit(0)
      ).otherwise(lit(1)).cast("short"))
      .select(
        $"point.*",
        $"x", $"y", $"z", $"position",
        $"uuid_edge",
        $"tags",
        $"prio"
      )
      .drop("geometry")

      dfPoints.as[PpWithMapping]
  }

}
object Tlm3dPpTransformer extends SmartDataLakeLogger {

  def createPointsAtFixedInterval(interval: Double, wellDefinedPointDistance: Double, srcCrs: String)(uuid_edge: String, geom: Geometry): Seq[EdgePoint] = try {
    implicit val geoFactory: GeometryFactory = getGeoFactory(srcCrs)
    if (geom.getNumPoints >= 2) {
      val coords = enrichLinePosition(geom.getCoordinates.toSeq, uuid_edge)
      val length = coords.last.getM
      val linePoints = createLinePointsWithRadius(coords, wellDefinedPointDistance)
      val linePointsQueue = mutable.Queue(linePoints:_*)
      val intervalPoints = (1 to math.floor((length / interval)-1).toInt).map { idx =>
        val position = idx * interval
        assert(position >= linePointsQueue(0).geometry.getM)
        while (position > linePointsQueue(1).geometry.getM) linePointsQueue.dequeue()
        assert(linePointsQueue.size >= 2)
        interpolatePoint(linePointsQueue(0), linePointsQueue(1), position)
      }
      val points = linePoints.head +: intervalPoints :+ linePoints.last
      val edgePoints = createEdgePointsWithGradeAndAzimuth(points)
      edgePoints
    } else {
      logger.error(s"Edge $uuid_edge has less than 2 points.")
      Seq()
    }
  } catch {
    case e: Throwable =>
      throw new RuntimeException(s"edge=$uuid_edge: ${e.getClass.getSimpleName}: ${e.getMessage}")
  }

  def interpolatePoint(p1: LinePoint, p2: LinePoint, position: Double): LinePoint = {
    val fraction = getFractionBetweenCoords(p1.geometry, p2.geometry, position)
    val coord = interpolateCoord(p1.geometry, p2.geometry, fraction, position)
    val radius = interpolateOptVal(p1.radius.map(_.toDouble), p2.radius.map(_.toDouble), fraction).map(_.toInt)
    LinePoint(coord, radius, p1.well_defined || p2.well_defined) // if one of both is well-defined, it's sufficient
  }

  def getZoom(pos: Double, length: Double): Short = {
    if (pos == 0d || pos == length) 0
    else if (math.abs(pos - math.round(pos / 10) * 10) < 0.01) 1
    else if (math.abs(pos - math.round(pos / 1) * 1) < 0.01) 2
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
  def createEdgePointsWithGradeAndAzimuth(points: Seq[LinePoint])(implicit geoFactory: GeometryFactory): Seq[EdgePoint] = {
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
          getZoom(current.geometry.getM, points.last.geometry.getM),
          current.radius, grade, azimuth, current.well_defined
        )
    }
  }
}

case class LinePoint(geometry: CoordinateXYZM, radius: Option[Int], well_defined: Boolean)

case class EdgePoint(geometry: Geometry, zoom: Short, radius: Option[Int], grade: Option[Float], azimuth: Float, well_defined: Boolean)

/**
 * @param x native coordinate 1
 * @param y native coordinate 2
 * @param z Height in m
 * @param zoom the zoom level of this Positionpoint
 * @param tags additional information
 * @param uuid_edge edge that this positionpoint is created from
 * @param position position on edge of this positionpoint
 * @param prio priority when creating unique positionpoints and their mapping to edges.
 *             Lower priorities get snapped first to existing positionpoints.
 *             This is important for switches, as we want the "main" edge to be merged first, so it has higher priority to create new positionpoints in the region of the "Weichenzunge".
 */
case class PpWithMapping(x: Double, y: Double, z: Float, zoom: Short, tags: Set[String], position: Double, uuid_edge: String, prio: Short, radius: Option[Int], grade: Option[Float], azimuth: Float, well_defined: Boolean) {
  @transient lazy val coordinate: Coordinate = new Coordinate(x,y,z)
  def getGeometry(implicit factory: GeometryFactory): Geometry = factory.createPoint(coordinate)
}