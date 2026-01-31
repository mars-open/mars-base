package ch.zzeekk.mars.tlm3d

import ch.zzeekk.mars.ch.tlm3d.{EdgePoint, Tlm3dPpTransformer}
import org.locationtech.jts.geom.{Coordinate, CoordinateXYZM, GeometryFactory, PrecisionModel}
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite

class Tlm3dPpTransformerTest extends AnyFunSuite {

  val lv95GeomFactory = new GeometryFactory(new PrecisionModel() , 2056)

  val line1 = {
    val coords = Array(
      new Coordinate(0, 0, 100d),
      new Coordinate(0, 0.7, 100d),
      new Coordinate(0, 1.3, 100d),
      new Coordinate(0, 2.05, 100d),
    )
    lv95GeomFactory.createLineString(coords)
  }

  val square1 = {
    val centerE = 0
    val centerN = 0
    val halfEdge = 0.5 // meter
    val coords = Array(
      new Coordinate(centerE - halfEdge, centerN - halfEdge, 100d), // bottom-left
      new Coordinate(centerE + halfEdge, centerN - halfEdge, 100d), // bottom-right
      new Coordinate(centerE + halfEdge, centerN + halfEdge, 110d), // top-right
      new Coordinate(centerE - halfEdge, centerN + halfEdge, 100d), // top-left
      new Coordinate(centerE - halfEdge, centerN - halfEdge, 100d)  // back to start
    )
    lv95GeomFactory.createLineString(coords)
  }

  def point(x: Double, y: Double, z: Double, m: Double) = lv95GeomFactory.createPoint(new CoordinateXYZM(x,y,z,m))

  test("EdgePoints for line1 are created in fixed interval of 0.25 distance, having a slightly larger gap in the middle") {
    val result = Tlm3dPpTransformer.createPointsAtFixedInterval(0.25, 25, "EPSG:2056")("line", line1)
    val expected = Seq(
      EdgePoint(point (0, 0, 100d, 0.0), 0, None, Some(0.0f), 1.5707964f,true, 0),
      EdgePoint(point (0, 0.25, 100d, 0.25), 3, None, Some(0.0f), 1.5707964f,true, 1),
      EdgePoint(point (0, 0.5, 100d, 0.5), 3, None, Some(0.0f), 1.5707964f,true, 2),
      EdgePoint(point (0, 0.75, 100d, 0.75), 3, None, Some(0.0f), 1.5707964f,true, 3),
      // here is the slightly larger gap of 0.3cm
      EdgePoint(point (0, 1.05, 100d, 1.05), 2, None, Some(0.0f), 1.5707964f,true, 4),
      EdgePoint(point (0, 1.3, 100d, 1.3), 3, None, Some(0.0f), 1.5707964f,true, 5),
      EdgePoint(point (0, 1.55, 100d, 1.55), 3, None, Some(0.0f), 1.5707964f,true, 6),
      EdgePoint(point (0, 1.8, 100d, 1.8), 3, None, Some(0.0f), 1.5707964f,true, 7),
      EdgePoint(point (0, 2.05, 100d, 2.05), 0, None, Some(0.0f), 1.5707964f,true, 8),
    )
    result.foreach(println)
    assert(result == expected)
    result.map(_.geometry).zip(expected.map(_.geometry)).foreach {
      case (p1, p2) =>
        val test = p1.getCoordinate.equals3D(p2.getCoordinate) &&
          p1.getCoordinate.getM == p2.getCoordinate.getM
        if (!test) {
          println(s"""
                     |Coordinates are not equal:
                     |  result: $p1 ${p1.getCoordinate.getM}
                     |  expected: $p2 ${p2.getCoordinate.getM}
                     |""".stripMargin)
        }
        assert(test)
    }
  }

  test("EdgePoints for square1 are created in fixed interval, including height/radius/grade/azimuth interpolation and position") {
    val result = Tlm3dPpTransformer.createPointsAtFixedInterval(0.25, 25, "EPSG:2056")("square", square1)
    val expected = Seq(
      EdgePoint(point (-0.5, -0.5, 100d, 0.0),   0, None,    Some(0.0f),          0.0f,true, 0),
      EdgePoint(point (-0.25, -0.5, 100d, 0.25), 3, Some(1), Some(0.0f),          0.0f,true, 1),
      EdgePoint(point (0, -0.5, 100d, 0.5),      3, Some(1), Some(0.0f),          0.0f,true, 2),
      EdgePoint(point (0.25, -0.5, 100d, 0.75),  3, Some(1), Some(0.0f),          0.0f,true, 3),
      EdgePoint(point (0.5, -0.5, 100d, 1.0),    2, Some(1), Some(0.0070710676f), 0.7853982f,true, 4),
      EdgePoint(point (0.5, -0.25, 102.5d, 1.25),3, Some(1), Some(0.01f),         1.5707964f,true, 5),
      EdgePoint(point (0.5, 0, 105d, 1.5),       3, Some(1), Some(0.01f),         1.5707964f,true, 6),
      EdgePoint(point (0.5, 0.25, 107.5d, 1.75), 3, Some(1), Some(0.01f),         1.5707964f,true, 7),
      EdgePoint(point (0.5, 0.5, 110d, 2.0),     2, Some(1), Some(0.0f),          2.3561945f,true, 8),
      EdgePoint(point (0.25, 0.5, 107.5d, 2.25), 3, Some(1), Some(-0.01f),        3.1415927f,true, 9),
      EdgePoint(point (0, 0.5, 105d, 2.5),       3, Some(1), Some(-0.01f),        3.1415927f,true, 10),
      EdgePoint(point (-0.25, 0.5, 102.5d, 2.75),3, Some(1), Some(-0.01f),        3.1415927f,true, 11),
      EdgePoint(point (-0.5, 0.5, 100d, 3),      2, Some(1), Some(-0.0070710676f),-2.3561945f,true, 12),
      EdgePoint(point (-0.5, 0.25, 100d, 3.25),  3, Some(1), Some(0.0f),          -1.5707964f,true, 13),
      EdgePoint(point (-0.5, 0, 100d, 3.5),      3, Some(1), Some(0.0f),          -1.5707964f,true, 14),
      EdgePoint(point (-0.5, -0.25, 100d, 3.75), 3, Some(1), Some(0.0f),          -1.5707964f,true, 15),
      EdgePoint(point (-0.5, -0.5, 100d, 4.0),   0, None,    Some(0.0f),          -1.5707964f,true, 16),
    )
    assert(result == expected)
    result.map(_.geometry).zip(expected.map(_.geometry)).foreach {
      case (p1, p2) =>
        val test = p1.getCoordinate.equals3D(p2.getCoordinate) &&
          p1.getCoordinate.getM == p2.getCoordinate.getM
        if (!test) {
          println(s"""
            |Coordinates are not equal:
            |  result: $p1 ${p1.getCoordinate.getM}
            |  expected: $p2 ${p2.getCoordinate.getM}
            |""".stripMargin)
        }
        assert(test)
    }
  }

  // TODO: hier gibt es eine grössere Lücke bei 20.5 (werden vielleicht beide auf 20.75 gesnapped?
  // Die Lücke ist hier noch nicht vorhanden, sondern entsteht wohl erst im SnapPpTransformer
  test("no missing points") {
    val reader = new WKTReader(lv95GeomFactory)
    val wktLinestring = "LINESTRING (2599549.034000003 1199612.9800117682, 2599515.9340000013 1199609.3600118787, 2599506.7240000004 1199608.5300119098, 2599489.0440000007 1199607.5100119407, 2599473.524000001 1199607.7500119316)"
    val geom = reader.read(wktLinestring)
    val result = Tlm3dPpTransformer.createPointsAtFixedInterval(0.25, 25, "EPSG:2056")("track", geom)
    result.map(_.geometry.getCoordinate).foreach(println)
    assert(result.forall(_.grade.isEmpty)) // no grade as wktLinestring is only 2D
    assert(result.size == math.floor(geom.getLength/0.25).toInt + 1)
  }

}
