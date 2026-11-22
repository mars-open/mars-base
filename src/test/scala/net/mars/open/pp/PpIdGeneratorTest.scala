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

import org.locationtech.jts.geom.{Coordinate, CoordinateXYZM, GeometryFactory, PrecisionModel}
import org.scalatest.funsuite.AnyFunSuite

class PpIdGeneratorTest extends AnyFunSuite {

  val lv95GeomFactory = new GeometryFactory(new PrecisionModel(), 2056)

  def point(x: Double, y: Double) = lv95GeomFactory.createPoint(new Coordinate(x, y))

  test("neighbouring cells share large part of the identifier") {
    val id1 = PpIdGenerator.getH3idL15(2600205.70, 1200190.64, "EPSG:2056")
    val id2 = PpIdGenerator.getH3idL15(2600200.90, 1200191.64, "EPSG:2056")
    println(s"h3id=$id1 baseAndDigits=${PpIdGenerator.getBaseAndDigits(id1)} token=${PpIdGenerator.getToken(id1)}")
    println(s"h3id=$id2 baseAndDigits=${PpIdGenerator.getBaseAndDigits(id2)} token=${PpIdGenerator.getToken(id2)}")
    assert(id1 >> 12 == id2 >> 12)
    assert(PpIdGenerator.getBaseAndDigits(id1) >> 10 == PpIdGenerator.getBaseAndDigits(id2) >> 10)
    assert(PpIdGenerator.getToken(id1).take(11) == PpIdGenerator.getToken(id2).take(11))
  }

  test("neighbour cells in range include all cells within distance, but not all neighbours") {
    import net.mars.open.pp.utils.GeometryCalcUtils.convertTo4326
    val srcCrs = "EPSG:2056"
    val radius = 0.25
    val rnd = new scala.util.Random(1)
    val nbOfNeighbours = (0 until 200).map { _ =>
      val (x, y) = (2600000 + rnd.nextDouble() * 100, 1200000 + rnd.nextDouble() * 100)
      val h3id = PpIdGenerator.getH3idL15(x, y, srcCrs)
      val wgs84 = convertTo4326(point(x, y), srcCrs).getCoordinate
      val cells = PpIdGenerator.getH3idL15WithNeighboursInRange(wgs84.getY, wgs84.getX, h3id, radius)
      // brute force: cells of a 1cm grid within the radius
      val step = 0.01
      val n = (radius / step).toInt
      val expected = (for (i <- -n to n; j <- -n to n if Math.hypot(i * step, j * step) < radius)
        yield PpIdGenerator.getH3idL15(x + i * step, y + j * step, srcCrs)).toSet
      assert(cells.head == h3id)
      assert(expected.subsetOf(cells.toSet), s"missing cells for ($x,$y): ${expected -- cells}")
      cells.size - 1
    }
    println(s"average number of neighbours in range: ${nbOfNeighbours.sum.toDouble / nbOfNeighbours.size}")
    assert(nbOfNeighbours.sum.toDouble / nbOfNeighbours.size < 3) // instead of 6 neighbours
  }

}
