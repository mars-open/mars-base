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
package ch.zzeekk.mars.tlm3d

import ch.zzeekk.mars.pp.{EdgePoint, Tlm3dPpTransformer}
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

  test("EdgePoints for line1 are created in fixed interval of 0.25 distance. Split left-over space of 0.3m at the each end.") {
    val result = Tlm3dPpTransformer.createPointsAtFixedInterval(0.25, 25, "EPSG:2056")("line", line1)
    val expected = Seq(
      EdgePoint(point (0, 0.15, 100d, 0.15), 1, None, Some(0.0f), 1.5707964f,true, 1),
      EdgePoint(point (0, 0.4, 100d, 0.4), 3, None, Some(0.0f), 1.5707964f,true, 2),
      EdgePoint(point (0, 0.65, 100d, 0.65), 3, None, Some(0.0f), 1.5707964f,true, 3),
      EdgePoint(point (0, 0.9, 100d, 0.9), 3, None, Some(0.0f), 1.5707964f,true, 4),
      EdgePoint(point (0, 1.15, 100d, 1.15), 2, None, Some(0.0f), 1.5707964f,true, 5),
      EdgePoint(point (0, 1.4, 100d, 1.4), 3, None, Some(0.0f), 1.5707964f,true, 6),
      EdgePoint(point (0, 1.65, 100d, 1.65), 3, None, Some(0.0f), 1.5707964f,true, 7),
      EdgePoint(point (0, 1.9, 100d, 1.9), 0, None, Some(0.0f), 1.5707964f,true, 8),
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

  test("EdgePoints for square1 are created in fixed interval, including height/radius/grade/azimuth interpolation and position") {1
    val result = Tlm3dPpTransformer.createPointsAtFixedInterval(0.25, 25, "EPSG:2056")("square", square1)
    val expected = Seq(
      EdgePoint(point (-0.375, -0.5, 100d, 0.125),   1, Some(1), Some(0.0f),           0.0f,        true, 1),
      EdgePoint(point (-0.125, -0.5, 100d, 0.375),   3, Some(1), Some(0.0f),           0.0f,        true, 2),
      EdgePoint(point (0.125, -0.5, 100d, 0.625),    3, Some(1), Some(0.0f),           0.0f,        true, 3),
      EdgePoint(point (0.375, -0.5, 100d, 0.875),    3, Some(1), Some(0.0031622776f),  0.32175055f,  true, 4),
      EdgePoint(point (0.5, -0.375, 101.25d, 1.125), 2, Some(1), Some(0.009486833f),   1.2490457f,   true, 5),
      EdgePoint(point (0.5, -0.125, 103.75d, 1.375), 3, Some(1), Some(0.01f),          1.5707964f,   true, 6),
      EdgePoint(point (0.5, 0.125, 106.25d, 1.625),  3, Some(1), Some(0.01f),          1.5707964f,   true, 7),
      EdgePoint(point (0.5, 0.375, 108.75d, 1.875),  3, Some(1), Some(0.0063245553f),  1.8925469f,   true, 8),
      EdgePoint(point (0.375, 0.5, 108.75d, 2.125),  2, Some(1), Some(-0.0063245553f), 2.819842f,    true, 9),
      EdgePoint(point (0.125, 0.5, 106.25d, 2.375),  3, Some(1), Some(-0.01f),         3.1415927f,   true, 10),
      EdgePoint(point (-0.125, 0.5, 103.75d, 2.625), 3, Some(1), Some(-0.01f),         3.1415927f,   true, 11),
      EdgePoint(point (-0.375, 0.5, 101.25d, 2.875), 3, Some(1), Some(-0.009486833f),  -2.819842f,   true, 12),
      EdgePoint(point (-0.5, 0.375, 100d, 3.125),    2, Some(1), Some(-0.0031622776f), -1.8925469f,  true, 13),
      EdgePoint(point (-0.5, 0.125, 100d, 3.375),    3, Some(1), Some(0.0f),           -1.5707964f,  true, 14),
      EdgePoint(point (-0.5, -0.125, 100d, 3.625),   3, Some(1), Some(0.0f),           -1.5707964f,  true, 15),
      EdgePoint(point (-0.5, -0.375, 100d, 3.875),   0, Some(1), Some(0.0f),           -1.5707964f,  true, 16),
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
    assert(result.forall(_.grade.isEmpty)) // no grade as wktLinestring is only 2D
    assert(result.size == math.floor(geom.getLength/0.25).toInt)
  }

}
