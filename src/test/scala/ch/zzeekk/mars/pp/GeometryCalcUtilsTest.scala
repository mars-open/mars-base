package ch.zzeekk.mars.pp

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils.{calcCircumRadius, enrichLinePosition, interpolateCoord}
import org.locationtech.jts.geom.{Coordinate, CoordinateXYZM}
import org.scalatest.funsuite.AnyFunSuite

class GeometryCalcUtilsTest extends AnyFunSuite {

  test("circumradius is positive when curve is to the right in LV95") {
    // Note: for LV95 y is rising from bottom to top
    val rLeft = calcCircumRadius(
      new Coordinate(0,0), new Coordinate(0,1), new Coordinate(1,1)
    )
    assert(rLeft.exists(_ < 0))

    val rRight = calcCircumRadius(
      new Coordinate(0,0), new Coordinate(0,1), new Coordinate(-1,1)
    )
    assert(rRight.exists(_ > 0))
  }

  test("circumradius when straight") {
    val r = calcCircumRadius(
      new Coordinate(0,0), new Coordinate(0,1), new Coordinate(0,2)
    )
    assert(r.isEmpty)
  }

  test("enrich line position") {
    val xs = Seq(new Coordinate(0,0), new Coordinate(0,1), new Coordinate(1,1))
    val result = enrichLinePosition(xs, "test")
    val expectedPositions = Seq(0.0, 1.0, 2.0)
    assert(result.map(_.getM) == expectedPositions)
  }

  test("interpolate point in range") {
    val c1 = new CoordinateXYZM(1.0, 1.0, 0.0, 2.0)
    val c2 = new CoordinateXYZM(3.0, 3.0, 2.0, 4.0)
    val result = interpolateCoord(c1, c2, 0.5, 3.0)
    val expected = new CoordinateXYZM(2.0, 2.0, 1.0, 3.0)
    assert(result == expected)
  }

  test("interpolate point fails if out of range") {
    val c1 = new CoordinateXYZM(1.0, 1.0, 0.0, 2.0)
    val c2 = new CoordinateXYZM(3.0, 3.0, 2.0, 4.0)
    intercept[AssertionError](interpolateCoord(c1, c2, 1.5, 4.5))
  }


}
