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
package ch.zzeekk.mars.pp

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
    assert(id1 >> 8 == id2 >> 8)
    assert(PpIdGenerator.getBaseAndDigits(id1) >> 8 == PpIdGenerator.getBaseAndDigits(id2) >> 8)
    assert(PpIdGenerator.getToken(id1).take(11) == PpIdGenerator.getToken(id2).take(11))
  }

}
