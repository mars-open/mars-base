package ch.zzeekk.mars.tlm3d

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils.{calcAzimuth, calcCircumRadius}
import ch.zzeekk.mars.pp.NodePoint
import org.locationtech.jts.geom.{Coordinate, CoordinateXYZM, GeometryFactory, PrecisionModel}
import org.scalatest.funsuite.AnyFunSuite

class Tlm3dNodeTransformerTest extends AnyFunSuite {

  val lv95GeomFactory = new GeometryFactory(new PrecisionModel(), 2056)

  def point(x: Double, y: Double, z: Double, m: Double) = lv95GeomFactory.createPoint(new CoordinateXYZM(x, y, z, m))

  val nodeTransformer = new Tlm3dNodeTransformer()

  test("Classify simply right switch") {

    val pCenter = point(0, 0, 0, 0)
    val p1 = point(-1, 0, 0, 0)
    val p21 = point(1, 0, 0, 0)
    val p22 = point(1, 1, 0, 0)
    val e1 = EdgeRef(
      NodePoint("p", pCenter, p1, calcAzimuth(pCenter.getCoordinate, p1.getCoordinate).toFloat, radius = None, chord_length = None, end = true),
      uuid_edge = "e1", edge_length = 25
    )
    val e2main = EdgeRef(
      NodePoint("p", pCenter, p21, calcAzimuth(pCenter.getCoordinate, p21.getCoordinate).toFloat, radius = None, chord_length = None, end = true),
      uuid_edge = "e21", edge_length = 30
    )
    val e2turnout = EdgeRef(
      NodePoint("p", pCenter, p22, calcAzimuth(pCenter.getCoordinate, p22.getCoordinate).toFloat, radius = calcCircumRadius(p1.getCoordinate, pCenter.getCoordinate, p22.getCoordinate).map(_.toInt), chord_length = Some(p1.distance(p22)).map(_.toFloat), end = false),
      uuid_edge = "e22", edge_length = 30
    )
    val (switch, edgeMapping) = nodeTransformer.classifySimpleSwitch(Seq(e1, e2main, e2turnout)).get
    val switchExpected = Switch("SS", Some("R"), Some(1))
    assert(switch == switchExpected)
    val edgeMappingExpected = Seq(
      EdgeMapping("e1", 0, None, None, None),
      EdgeMapping("e21", 1, Some(10.0), Some(30.0), Some(true)),
      EdgeMapping("e22", 1, Some(0.0), Some(20.0), Some(false))
    )
    assert(edgeMapping == edgeMappingExpected)
  }

  test("Classify symmetric switch") {

    val pCenter = point(0, 0, 0, 0)
    val p1 = point(-1, 0, 0, 0)
    val p21 = point(1, -0.5, 0, 0)
    val p22 = point(1, 0.55, 0, 0)
    val e1 = EdgeRef(
      NodePoint("p", pCenter, p1, calcAzimuth(pCenter.getCoordinate, p1.getCoordinate).toFloat, radius = None, chord_length = None, end = true),
      uuid_edge = "e1", edge_length = 25
    )
    val e2main = EdgeRef(
      NodePoint("p", pCenter, p21, calcAzimuth(pCenter.getCoordinate, p21.getCoordinate).toFloat, radius = None, chord_length = None, end = true),
      uuid_edge = "e21", edge_length = 30
    )
    val e2turnout = EdgeRef(
      NodePoint("p", pCenter, p22, calcAzimuth(pCenter.getCoordinate, p22.getCoordinate).toFloat, radius = calcCircumRadius(p1.getCoordinate, pCenter.getCoordinate, p22.getCoordinate).map(_.toInt), chord_length = Some(p1.distance(p22)).map(_.toFloat), end = false),
      uuid_edge = "e22", edge_length = 30
    )

    val (switch, edgeMapping) = nodeTransformer.classifySimpleSwitch(Seq(e1, e2main, e2turnout)).get

    val switchExpected = Switch("SS", Some("LR"), Some(2))
    assert(switch == switchExpected)

    val edgeMappingExpected = Seq(
      EdgeMapping("e1", 0, None, None, None),
      EdgeMapping("e21", 1, Some(10.0), Some(30.0), Some(true)),
      EdgeMapping("e22", 1, Some(0.0), Some(20.0), Some(false))
    )
    assert(edgeMapping == edgeMappingExpected)
  }


  test("Classify curve left switch") {

    val pCenter = point(0, 0, 0, 0)
    val p1 = point(-1, 0, 0, 0)
    val p21 = point(1, -0.5, 0, 0)
    val p22 = point(1, -1.5, 0, 0)
    val e1 = EdgeRef(
      NodePoint("p", pCenter, p1, calcAzimuth(pCenter.getCoordinate, p1.getCoordinate).toFloat, radius = None, chord_length = None, end = true),
      uuid_edge = "e1", edge_length = 25
    )
    val e2main = EdgeRef(
      NodePoint("p", pCenter, p21, calcAzimuth(pCenter.getCoordinate, p21.getCoordinate).toFloat, radius = None, chord_length = None, end = true),
      uuid_edge = "e21", edge_length = 30
    )
    val e2turnout = EdgeRef(
      NodePoint("p", pCenter, p22, calcAzimuth(pCenter.getCoordinate, p22.getCoordinate).toFloat, radius = calcCircumRadius(p1.getCoordinate, pCenter.getCoordinate, p22.getCoordinate).map(_.toInt), chord_length = Some(p1.distance(p22)).map(_.toFloat), end = false),
      uuid_edge = "e22", edge_length = 30
    )

    val (switch, edgeMapping) = nodeTransformer.classifySimpleSwitch(Seq(e1, e2main, e2turnout)).get

    val switchExpected = Switch("SS", Some("LL"), Some(1))
    assert(switch == switchExpected)

    val edgeMappingExpected = Seq(
      EdgeMapping("e1", 0, None, None, None),
      EdgeMapping("e21", 1, Some(10.0), Some(30.0), Some(true)),
      EdgeMapping("e22", 1, Some(0.0), Some(20.0), Some(false))
    )
    assert(edgeMapping == edgeMappingExpected)
  }


  test("Classify double crossing") {

    val pCenter = point(0, 0, 0, 0)
    val p11 = point(-1, 0, 0, 0)
    val p12 = point(-1, -1, 0, 0)
    val p21 = point(1, 0, 0, 0)
    val p22 = point(1, 1, 0, 0)
    val e1main = EdgeRef(
      NodePoint("p", pCenter, p11, calcAzimuth(pCenter.getCoordinate, p11.getCoordinate).toFloat, radius = None, chord_length = None, end = false),
      uuid_edge = "e11", edge_length = 50 // main is the longer edge than the other
    )
    val e1other = EdgeRef(
      NodePoint("p", pCenter, p12, calcAzimuth(pCenter.getCoordinate, p12.getCoordinate).toFloat, radius = None, chord_length = None, end = true),
      uuid_edge = "e12", edge_length = 25
    )
    val e2main = EdgeRef(
      NodePoint("p", pCenter, p21, calcAzimuth(pCenter.getCoordinate, p21.getCoordinate).toFloat, radius = None, chord_length = None, end = false),
      uuid_edge = "e21", edge_length = 50
    )
    val e2other = EdgeRef(
      NodePoint("p", pCenter, p22, calcAzimuth(pCenter.getCoordinate, p22.getCoordinate).toFloat, radius = None, chord_length = None, end = true),
      uuid_edge = "e22", edge_length = 25
    )

    val (switch, edgeMapping) = nodeTransformer.classifyDoubleSwitch(Seq(e1main, e1other,  e2main, e2other)).get

    val switchExpected = Switch("DCS", None, Some(1))
    assert(switch == switchExpected)

    val edgeMappingExpected = Seq(
      EdgeMapping("e11", 0, Some(0.0), Some(10.0), Some(true)),
      EdgeMapping("e12", 0, Some(15.0), Some(25.0), Some(false)),
      EdgeMapping("e21", 1, Some(0.0), Some(10.0), Some(true)),
      EdgeMapping("e22", 1, Some(15.0), Some(25.0), Some(false))
    )
    assert(edgeMapping == edgeMappingExpected)
  }
}
