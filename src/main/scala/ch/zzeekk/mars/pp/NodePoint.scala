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

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils.{calcAzimuth, calcCircumRadius}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}

/**
 * @param geometry Point geometry
 * @param geometry1 First point on the edge
 * @param azimuth normalized in the range of [-Pi,Pi]
 * @param radius of the track, starting from point.
 *               Negative if curve is to the left, and positive if to the right (LV95 coordinate system)
 * @param chord_length length of the chord to calculate the radius in m. Can be used to qualify radius information.
 * @param end true if this is the end-point of the geometry. If false it is the start point.
 */
case class NodePoint(uuid_node: String, geometry: Geometry, geometry1: Geometry, azimuth: Float, radius: Option[Int], chord_length: Option[Float], end: Boolean) {
  def point: Point = geometry.asInstanceOf[Point]
}
object NodePoint {

  def from(uuid_node: String, p1: Coordinate, p2: Coordinate, p3: Option[Coordinate], end: Boolean)(implicit factory: GeometryFactory): NodePoint = {
    val azimuth = calcAzimuth(p1, p2).toFloat
    val circum = p3.map(p3 => (calcCircumRadius(p1, p2, p3).map(_.toFloat), p1.distance(p3).toFloat))
    NodePoint(uuid_node, factory.createPoint(p1), factory.createPoint(p2), azimuth, circum.flatMap(_._1).map(_.toInt), circum.map(_._2), end)
  }
}