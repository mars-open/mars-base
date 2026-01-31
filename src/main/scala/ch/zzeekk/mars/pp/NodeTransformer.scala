package ch.zzeekk.mars.pp

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions.ST_Length
import org.apache.spark.sql.{Dataset, SparkSession}
import org.locationtech.jts.algorithm.Angle
import org.locationtech.jts.geom.{Geometry, GeometryFactory}

/**
 * Create unique nodes from endpoints of edges.
 * Classify the switches.
 */
class NodeTransformer extends CustomDfsTransformer {

  def transform(dsSlvTlm3dEdge: Dataset[Edge]): Dataset[Node] = {
    implicit val session: SparkSession = dsSlvTlm3dEdge.sparkSession
    import session.implicits._

    // extract start- and endpoint of each track, including angle
    val udfExtractStartEndPoint = udf(extractStartEndPoint _)
    val dfNodePoints = dsSlvTlm3dEdge
      .withColumn("points", udfExtractStartEndPoint($"geometry", $"uuid_node_from", $"uuid_node_to"))
      .withColumn("endpoint", explode(array($"points._1", $"points._2")))
      .select(
        $"endpoint",
        $"uuid_edge",
        ST_Length($"geometry").as("edge_length"),
        $"tags"
      )

    val udfClassifySimpleSwitch = udf(classifySimpleSwitch _)
    val udfClassifyDoubleCrossingSwitch = udf(classifyDoubleSwitch _)
    val dfNodePrep = dfNodePoints
      // ignore endpoints from edges with only one point (if any...)
      .where($"endpoint.azimuth".isNotNull)
      // group nodes
      .groupBy($"endpoint.uuid_node", $"endpoint.geometry")
      .agg(
        collect_list(struct($"endpoint", $"uuid_edge", $"edge_length")).as("edges"), // collect edge informations
        array_distinct(flatten(collect_list($"tags"))).as("tags") // combine tags from all edges
      )
      .withColumn("arity", size($"edges").cast("short"))
      .withColumn("class",
        when($"arity" === 3, udfClassifySimpleSwitch($"edges"))
          .when($"arity" === 4, udfClassifyDoubleCrossingSwitch($"edges"))
      )

    val dfNode = dfNodePrep
      .select(
        $"uuid_node", $"geometry", $"arity", $"class._1".as("switch_type"), $"class._2".as("edges"), $"tags"
      )
      .as[Node]

    dfNode
  }

  def classifySimpleSwitch(edges: Seq[EdgeRef]): Option[(Switch, Seq[EdgeMapping])] = {
    // cluster nodes into two groups
    // c1 is the smaller group on the side of the "tongue"
    val Seq(c1,c2) = clusterEdgesK2(edges).sortBy(_.size)
    (c1.size, c2.size) match {
      case (1, 2) =>
        val edge1 = c1.head
        val switchAzimuth = edge1.endpoint.azimuth - Math.PI
        // main edge of the switch is the one that is more aligned with c1 edge
        val mainEdge = c2.minBy(e => Angle.diff(switchAzimuth, e.endpoint.azimuth))
        val turnoutEdge = c2.diff(Seq(mainEdge)).head
        val radius = GeometryCalcUtils.calcCircumRadius(edge1.endpoint.geometry1.getCoordinate, edge1.endpoint.geometry.getCoordinate, turnoutEdge.endpoint.geometry1.getCoordinate).map(_.toInt)
        // define simple switch
        val switch = Switch(
          tpe = "SS",
          sub_tpe = getSubTypeFromTracks(switchAzimuth, c2),
          radius = Seq(turnoutEdge.endpoint.radius, radius).flatten.map(Math.abs).minOption
        )
        // refine edges
        // TODO: refine switch length based on ???
        val defaultSwitchLength = 20
        val switchLength = Math.min(c2.map(_.edge_length).min, defaultSwitchLength).toFloat
        val edgesExtended =
          c1.map(t => t.createMapping(side = 0)) ++
            c2.map(t => t.createMapping(side = 1, main_edge = Some(t.uuid_edge == mainEdge.uuid_edge), switch_length = Some(switchLength)))
        Some(switch, edgesExtended)
      case (n1, n2) =>
        logger.warn(s"Strange simple switch at ${edges.head.endpoint.point.getCoordinate}: clusterEdgesK2=($n1,$n2)")
        None
    }
  }

  def classifyDoubleSwitch(edges: Seq[EdgeRef]): Option[(Switch, Seq[EdgeMapping])] = {
    // cluster nodes into two groups
    val Seq(c1,c2) = clusterEdgesK2(edges).sortBy(_.size)

    (c1.size, c2.size) match {

      // double crossing switch
      case (2,2) =>
        // main edge of cluster 1 is the longest.
        val mainEdge1 = c1.maxBy(e => e.edge_length)
        val turnoutEdge1 = c1.diff(Seq(mainEdge1)).head
        val switchAzimuth = mainEdge1.endpoint.azimuth - Math.PI
        val mainEdge2 = c2.minBy(e => Angle.diff(switchAzimuth, e.endpoint.azimuth))
        val turnoutEdge2 = c2.diff(Seq(mainEdge2)).head
        val radius = GeometryCalcUtils.calcCircumRadius(mainEdge1.endpoint.geometry1.getCoordinate, mainEdge1.endpoint.geometry.getCoordinate, turnoutEdge2.endpoint.geometry1.getCoordinate).map(_.toInt)
        val switch = Switch(
          tpe="DCS",
          sub_tpe = None,
          radius = radius.map(Math.abs)
        )
        // TODO: refine switch length based on ???
        val defaultSwitchLength = 10 // 2x 10m on both sides
        val switchLength = Math.min(c2.map(_.edge_length).min, defaultSwitchLength).toFloat
        val edgesExtended =
          c1.map(t => t.createMapping(side = 0, main_edge = Some(t.uuid_edge==mainEdge1.uuid_edge), switch_length = Some(switchLength))) ++
          c2.map(t => t.createMapping(side = 1, main_edge = Some(t.uuid_edge==mainEdge2.uuid_edge), switch_length = Some(switchLength)))
        Some(switch, edgesExtended)

      // double switch
      case (1,3) =>
        val edge1 = c1.head
        val switchAzimuth = edge1.endpoint.azimuth - Math.PI
        // main edge of the switch is the one that is more aligned with c1 edge
        val mainEdge = c2.minBy(e => Angle.diff(switchAzimuth, e.endpoint.azimuth))
        val turnoutEdge = c2.diff(Seq(mainEdge)).head
        val radius = GeometryCalcUtils.calcCircumRadius(edge1.endpoint.geometry1.getCoordinate, edge1.endpoint.geometry.getCoordinate, turnoutEdge.endpoint.geometry1.getCoordinate).map(_.toInt)
        val switch = Switch(
          tpe="DS",
          sub_tpe = getSubTypeFromTracks(switchAzimuth, c2),
          radius = Seq(turnoutEdge.endpoint.radius, radius).flatten.map(Math.abs).minOption
        )
        // refine edges
        // TODO: refine switch length based on ???
        val defaultSwitchLength = 20
        val switchLength = Math.min(c2.map(_.edge_length).min, defaultSwitchLength).toFloat
        val edgesExtended =
          c1.map(_.createMapping(side = 0)) ++
          c2.map(t => t.createMapping(side = 1, main_edge = Some(t.uuid_edge==mainEdge.uuid_edge), switch_length = Some(switchLength)))
        Some(switch, edgesExtended)

      case (n1,n2) =>
        logger.warn(s"Strange double switch at ${edges.head.endpoint.point.getCoordinate}: clusterEdgesK2=($n1,$n2)")
        None
    }
  }


  /**
   * Summarize direction as L/R of the different tracks, sorted by radius
   * -> LL, L, R, RR
   */
  def getSubTypeFromTracks(azimuth: Double, tracks: Seq[EdgeRef]): Option[String] = {
    val subType = tracks
      .map(t => Angle.normalize(t.endpoint.azimuth - azimuth))
      .filter(t => Math.abs(t) >= Math.PI/180) // smaller than 1grad
      .sorted
      .map(diff => if (diff < 0) 'R' else 'L')
      .mkString
    if (subType.isEmpty) None else Some(subType)
  }

  /**
   * Cluster node tracks into 2 groups
   * assumption: angle difference is bigger than 90 degrees between the groups
   */
  def clusterEdgesK2(edges: Seq[EdgeRef]): Seq[Seq[EdgeRef]] = {
    def angleDist(c: Seq[EdgeRef], track: EdgeRef) = {
      Angle.diff(c.head.endpoint.azimuth, track.endpoint.azimuth)
    }
    val (c1, c2) = edges.foldLeft((Seq[EdgeRef](), Seq[EdgeRef]())) {
      case ((c1,c2), track) =>
        if (c1.isEmpty || angleDist(c1, track) < Math.PI / 2) (c1 :+ track, c2)
        else (c1, c2 :+ track)
    }
    Seq(c1,c2)
  }

  def extractStartEndPoint(geom: Geometry, uuidNodeFrom: String, uuidNodeTo: String): Option[(NodePoint, NodePoint)] = {
    implicit val factory: GeometryFactory = geom.getFactory

    // start
    val startCoord = geom.getCoordinates.apply(0)
    val secondCoord = geom.getCoordinates.apply(1)
    val thirdCoord = if(geom.getNumPoints>2) Some(geom.getCoordinates.apply(2)) else None
    val startPoint = NodePoint.from(uuidNodeFrom, startCoord, secondCoord, thirdCoord, end=false)

    // end
    val endCoord = geom.getCoordinates.apply(geom.getNumPoints -1)
    val secondLastCoord = geom.getCoordinates.apply(geom.getNumPoints -2)
    val thirdLastCoord = if(geom.getNumPoints>2) Some(geom.getCoordinates.apply(geom.getNumPoints -3)) else None
    val endPoint = NodePoint.from(uuidNodeTo, endCoord, secondLastCoord, thirdLastCoord, end=true)

    Some(startPoint, endPoint)
  }

}

/**
 * @param uuid_node generated id of the node
 * @param geometry point geometry of the node
 * @param arity number of connected edges
 * @param switch_type classification of the switch
 * @param edges mapping of the switch on the edges
 */
case class Node(uuid_node: String, geometry: Geometry, arity: Short, switch_type: Switch, edges: Seq[EdgeMapping], tags: Seq[String])

/**
 * @param tpe Basic type of the switch, e.g. SS -> Simple Switch, DS -> Double Switch, DCS -> Double Crossing Switch
 * @param sub_tpe Summary of curve as L/R of the different edges, sorted by radius: LL, L, R, RR
 * @param radius Radius of the switch
 */
case class Switch(tpe: String, sub_tpe: Option[String], radius: Option[Int])

case class EdgeRef(endpoint: NodePoint, uuid_edge: String, edge_length: Double) {
  def createMapping(side: Short, main_edge: Option[Boolean] = None, switch_length: Option[Float] = None): EdgeMapping = {
    val position_from = switch_length.map(l => if (endpoint.end) edge_length - l else 0d)
    val position_to = switch_length.map(l => if (endpoint.end) edge_length else l)
    EdgeMapping(uuid_edge, side, position_from, position_to, main_edge)
  }
}

/**
 * @param uuid_edge id of the edge
 * @param node_side side of the node this edge got assigned to.
 *                  Note: the graph traversal rule is that at a node the side needs to be switched, e.g. the next edge must start on the other side of the node.
 * @param position_from start position of the switch on this edge (this is always the smaller position value)
 * @param position_to end position of the switch on this edge (this is always the larger position value)
 * @param main_edge true if this is the main edge of the switch, if false it is a turnout edge.
 */
case class EdgeMapping(uuid_edge: String, node_side: Short, position_from: Option[Double], position_to: Option[Double], main_edge: Option[Boolean])