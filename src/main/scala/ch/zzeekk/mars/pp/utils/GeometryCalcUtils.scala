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
package ch.zzeekk.mars.pp.utils

import org.geotools.api.referencing.operation.MathTransform
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.locationtech.jts.algorithm.Angle
import org.locationtech.jts.geom._

import scala.collection.mutable

object GeometryCalcUtils {

  /**
   * Radius des Umkreises
   * see also https://de.wikipedia.org/wiki/Umkreis
   * In dieser Implementation kann der Radius negative und positive sein, abhängig ob die Kurve nach links oder rechts geht.
   * Achtung: Links/Rechts ist abhängig vom Koordinatensystem; für LV95 ist negative=links und positive=rechts (as y is rising from bottom to top)
   * Wenn die Koordinaten gerade sind, wird der Umkreis Radius infinite; die Funktione wandelt ihn ab 100000m in None um.
   */
  def calcCircumRadius(a: Coordinate, b: Coordinate, c: Coordinate): Option[Double] = {
    val ab = a.distance(b)
    val bc = b.distance(c)
    val ca = c.distance(a)
    val signedArea = 0.5 * (a.x * (b.y - c.y) + b.x * (c.y - a.y) + c.x * (a.y - b.y))
    val r = (ab * bc * ca) / (4 * signedArea)
    if (r.isInfinite || math.abs(r) > 100000) None else Some(r)
  }

  /**
   * Grade between two points as mm/m (Promille)
   */
  def calcGrade(c1: Coordinate, c2: Coordinate): Option[Double] = {
    val distXY = c1.distance(c2)
    if (distXY == 0) return None
    val distZ = (Option(c1.getZ), Option(c2.getZ)) match {
      case (Some(z1), Some(z2)) => Some(z2 - z1)
      case _ => None
    }
    distZ.map(distZ => distZ / distXY / 1000)
  }

  /**
   * Azimut between two points as angle with range [-Pi,+Pi]
   */
  def calcAzimuth(c1: Coordinate, c2: Coordinate): Double = {
    Angle.angle(c1, c2)
  }

  /**
   * Checks if start Azimuth of geom2 is an extension of end Azimuth of geom1, i.e. if the angle between them is smaller than 45°.
   */
  def isExtension(geom1: LineString, geom2: LineString): Boolean ={
    angleDiff(geom1, geom2) < math.Pi / 4
  }

  /**
   * Calculate angle between end of geom1 and beginning of geom2
   */
  def angleDiff(geom1: LineString, geom2: LineString): Double ={
    val azimuth1 = calcAzimuth(geom1.getCoordinates.apply(geom1.getCoordinates.length - 2), geom1.getCoordinates.last)
    val azimuth2 = calcAzimuth(geom2.getCoordinates.head, geom2.getCoordinates.apply(1))
    Angle.diff(azimuth1, azimuth2)
  }

  /**
   * Calculates the position of each Coordinate in a line represented as List of Coordinates.
   * The position is stored as the measure value of the Coordinates.
   */
  def enrichLinePosition(xs: Seq[Coordinate], uuid: String): Seq[CoordinateXYZM] = {
    xs.scanLeft(Option.empty[CoordinateXYZM]) {
      case (None, coord) => Some(new CoordinateXYZM(coord.x, coord.y, coord.z, 0d)) // initialize first coordinate with position 0d
      case (Some(prevCoord), coord) =>
        val distance = prevCoord.distance(coord)
        assert(distance > 0, s"Edge $uuid as duplicate point $coord")
        Some(new CoordinateXYZM(coord.x, coord.y, coord.z, prevCoord.getM + distance))
    }.flatten
  }

  @inline
  def interpolateOptVal(v1: Option[Double], v2: Option[Double], fraction: Double): Option[Double] = (v1,v2) match {
    case (Some(v1),Some(v2)) => Some(interpolateVal(v1, v2, fraction))
    case (_, _) => v1.orElse(v2)
  }

  @inline
  def interpolateVal(v1: Double, v2: Double, fraction: Double): Double = {
    v1 + fraction * (v2 - v1)
  }

  def interpolateCoord(c1: Coordinate, c2: Coordinate, fraction: Double, pos: Double): CoordinateXYZM = {
    assert(0d <= fraction && fraction <= 1d)
    new CoordinateXYZM(
      interpolateVal(c1.x, c2.x, fraction),
      interpolateVal(c1.y, c2.y, fraction),
      interpolateVal(c1.z, c2.z, fraction),
      pos
    )
  }

  def splitAcuteGeometry(
                          geometry: Geometry,
                          maxAngle: Double = math.Pi / 3,
                          factoryMethod: (Array[Coordinate], GeometryFactory) => Geometry = (coords, factory) => factory.createLineString(coords)
                        ): Seq[Geometry] = {
    val (segments, currSegment) = geometry.getCoordinates.foldLeft(Vector[Vector[Coordinate]](), Vector[Coordinate]()) {
      case ((segments, currSegment), coord) =>
        if (currSegment.size < 2) (segments, currSegment :+ coord)
        else {
          val angle = Angle.angleBetween(currSegment(currSegment.size - 2), currSegment.last, coord)
          if (angle <= maxAngle) (segments :+ currSegment, Vector(currSegment.last) :+ coord)
          else (segments, currSegment :+ coord)
        }
    }
    (segments :+ currSegment)
      .map(segment => factoryMethod(segment.toArray, geometry.getFactory))
  }

  def mergeLineStrings(lineString1: LineString, lineString2: LineString): LineString = {
    assert(lineString1.getEndPoint.equals(lineString2.getStartPoint), s"LineStrings cannot be merged as they are not connected: ${lineString1.getEndPoint} != ${lineString2.getStartPoint}")
    lineString1.getFactory.createLineString(lineString1.getCoordinates ++ lineString2.getCoordinates.tail)
  }

  def getFractionBetweenCoords(c1: Coordinate, c2: Coordinate, pos: Double): Double = {
    assert(c1.getM <= pos && pos <= c2.getM, s"m=$pos not between $c1 and $c2")
    (pos - c1.getM) / (c2.getM - c1.getM)
  }

  def getSridFromCrs(crs: String): Int = {
    val epsgRegex = "EPSG:([0-9]+)".r.anchored
    crs match {
      case epsgRegex(srid) => srid.toInt
    }
  }

  def getGeoFactory(crs: String): GeometryFactory = {
    geoFactories.getOrElseUpdate(crs, new GeometryFactory(stdPrecisionModel, getSridFromCrs(crs)))
  }
  @transient private lazy val geoFactories = mutable.Map[String,GeometryFactory]()
  @transient private lazy val stdPrecisionModel = new PrecisionModel()

  @inline
  def convert4326to3857(geometry: Geometry): Geometry = {
    JTS.transform(geometry, _4326to3857transform)
  }
  @transient private lazy val _4326to3857transform = getCrsTransform("EPSG:4326", "EPSG:3857")

  @inline
  def convertTo4326(geometry: Geometry, srcCrs: String): Geometry = {
    JTS.transform(geometry, getCrsTransform(srcCrs, "EPSG:4326"))
  }

  @inline
  def convertCrs(geometry: Geometry, srcCrs: String, tgtCrs: String): Geometry = {
    JTS.transform(geometry, getCrsTransform(srcCrs, tgtCrs))
  }

  @inline
  def getCrsTransform(srcCrs: String, tgtCrs: String): MathTransform = {
    crsTransforms.getOrElseUpdate((srcCrs,tgtCrs), CRS.findMathTransform(CRS.decode(srcCrs, true), CRS.decode(tgtCrs, true), false))
  }
  @transient private lazy val crsTransforms = mutable.Map[(String,String),MathTransform]()
}
