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
package net.mars.open.osm

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import org.scalatest.funsuite.AnyFunSuite

class OsmTrackTransformerTest extends AnyFunSuite {

  private val geomFactory = new GeometryFactory(new PrecisionModel(), 2056)

  private def line(xs: Seq[Double]) = {
    geomFactory.createLineString(xs.map(x => new Coordinate(x, 0d)).toArray)
  }

  test("splitTrack splits multiple times at non-adjacent intermediate switch node ids") {
    val geometry = line(Seq(0, 1, 2, 3, 4, 5))
    val nodeIds = Seq[Long](100L, 101L, 102L, 103L, 104L, 105L)

    val result = OsmTrackTransformer.splitTrack(geometry, nodeIds, id = 1000L, switchIds = Set(102L, 104L, 999L))

    assert(result.size == 3)
    assert(result.map(_.nodeFrom) == Seq(100L, 102L, 104L))
    assert(result.map(_.nodeTo) == Seq(102L, 104L, 105L))
    assert(result.map(_.geometry.getNumPoints) == Seq(3, 3, 2))
  }

  test("splitTrack asserts with track id when nodeIds size does not match geometry point count") {
    val geometry = line(Seq(0, 1, 2))          // 3 points
    val nodeIds = Seq[Long](100L, 101L)        // 2 ids — mismatch

    val ex = intercept[AssertionError] {
      OsmTrackTransformer.splitTrack(geometry, nodeIds, id = 42L, switchIds = Set.empty)
    }
    assert(ex.getMessage.contains("42"))
    assert(ex.getMessage.contains("2"))
    assert(ex.getMessage.contains("3"))
  }

  test("splitTrack keeps one segment when no intermediate switch exists") {
    val geometry = line(Seq(0, 1, 2))
    val nodeIds = Seq[Long](100L, 101L, 102L)

    val result = OsmTrackTransformer.splitTrack(geometry, nodeIds, id = 1000L, switchIds = Set(100L, 102L, 999L))

    assert(result.size == 1)
    assert(result.head.nodeFrom == 100L)
    assert(result.head.nodeTo == 102L)
    assert(result.head.geometry.getNumPoints == 3)
  }

  test("splitTrack creates a valid two-point segment between adjacent switch nodes for reported OSM track") {
    val geometry = geomFactory.createLineString(Array(
      new Coordinate(477902.5318933718, 5551446.405076335),
      new Coordinate(477893.2177584411, 5551446.877273026),
      new Coordinate(477857.80896792037, 5551447.791219088),
      new Coordinate(477846.73326907225, 5551447.603673106),
      new Coordinate(477825.27537981205, 5551447.225769354),
      new Coordinate(477807.64085958584, 5551446.431778207),
      new Coordinate(477785.9074830359, 5551445.143432805),
      new Coordinate(477785.6714073201, 5551445.1110586235),
      new Coordinate(477734.7623844028, 5551434.115348777),
      new Coordinate(477714.2757603098, 5551430.0646218285),
      new Coordinate(477699.55881217204, 5551427.691051669),
      new Coordinate(477689.3595405278, 5551426.7996837245)
    ))
    val nodeIds = Seq[Long](3893521084L, 3893521085L, 3893520406L, 3893521088L, 3893521086L, 3893521082L, 3893521078L, 3893521076L, 13322955917L, 3893520746L, 13322955916L, 3893520741L)

    val result = OsmTrackTransformer.splitTrack(geometry, nodeIds, id = 386007864L, switchIds = Set(3893521076L, 3893521078L))

    assert(result.size == 3)
    assert(result.map(_.nodeFrom) == Seq(3893521084L, 3893521078L, 3893521076L))
    assert(result.map(_.nodeTo) == Seq(3893521078L, 3893521076L, 3893520741L))
    assert(result.map(_.geometry.getNumPoints) == Seq(7, 2, 5))
    assert(result.forall(_.geometry.getNumPoints >= 2))
  }
}
