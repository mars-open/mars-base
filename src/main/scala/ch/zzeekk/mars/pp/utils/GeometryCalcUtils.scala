package ch.zzeekk.mars.pp.utils

import org.geotools.api.referencing.operation.MathTransform
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.locationtech.jts.algorithm.Angle
import org.locationtech.jts.geom.{Coordinate, CoordinateXYZM, Geometry, GeometryFactory, PrecisionModel}

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
