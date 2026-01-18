package ch.zzeekk.mars.pp

import com.uber.h3core.H3Core
import org.locationtech.jts.geom.{CoordinateXYZM, GeometryFactory, PrecisionModel}
import org.scalatest.funsuite.AnyFunSuite

class SnapPpTransformerTest extends AnyFunSuite {

  val lv95GeomFactory = new GeometryFactory(new PrecisionModel(), 2056)

  def point(x: Double, y: Double, z: Double, m: Double) = lv95GeomFactory.createPoint(new CoordinateXYZM(x, y, z, m))

  val transformer = new SnapPpTransformer()

  val ppRadius = 0.25f
  val ppHeightTolerance = 1f
  val srcCrs = "EPSG:2056"

  val h3: H3Core = H3Core.newInstance
  val h3IdSampleAddress = "8f1f835526a89a1"
  val h3IdSample: Long = h3.stringToH3(h3IdSampleAddress)

  test("single point should snap to existing pp when in range") {

    val idGenerator = new PpIdGenerator(0, h3IdSample)

    val newPpms = Seq(
      PpWithMapping(0, 0, 0, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
    )
    val existingPps = Seq(
      Pp.from(newPpms.head, h3IdSample, idGenerator.nextPpId, srcCrs)
        .copy(x=0.12, y=0.1)
    )
    val (newPps1, snappedPps1) = transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(newPpms, existingPps, h3IdSample)
      .partition(_.is_new_pp)
    assert(newPps1.isEmpty)
    assert(snappedPps1.size == 1)
    assert(newPps1.forall(_.direction.contains(1)))

  }

  test("single point should snap to existing pp in reverse direction") {

    val idGenerator = new PpIdGenerator(0, h3IdSample)

    val newPpms = Seq(
      PpWithMapping(0, 0, 0, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
    )
    val existingPps = Seq(
      Pp.from(newPpms.head, h3IdSample, idGenerator.nextPpId, srcCrs)
        .copy(azimuth = Math.PI.toFloat)
    )
    val (newPps1, snappedPps1) = transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(newPpms, existingPps, h3IdSample)
      .partition(_.is_new_pp)
    assert(newPps1.isEmpty)
    assert(snappedPps1.size == 1)
    assert(newPps1.forall(_.direction.contains(-1)))

  }

  test("single point should snap to existing pp without direction") {

    val idGenerator = new PpIdGenerator(0, h3IdSample)

    val newPpms = Seq(
      PpWithMapping(0, 0, 0, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
    )
    val existingPps = Seq(
      Pp.from(newPpms.head, h3IdSample, idGenerator.nextPpId, srcCrs)
        .copy(azimuth = Math.PI.toFloat / 2)
    )
    val (newPps1, snappedPps1) = transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(newPpms, existingPps, h3IdSample)
      .partition(_.is_new_pp)
    assert(newPps1.isEmpty)
    assert(snappedPps1.size == 1)
    assert(newPps1.forall(_.direction.isEmpty))

  }

  test("single point should not snap to existing pp if not in range") {

    val idGenerator = new PpIdGenerator(0, h3IdSample)

    val newPpms = Seq(
      PpWithMapping(0, 0, 0, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
    )
    val existingPps = Seq(
      Pp.from(newPpms.head, h3IdSample, idGenerator.nextPpId, srcCrs)
        .copy(x=0.27, y=0.1)
    )
    val (newPps1, snappedPps1) = transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(newPpms, existingPps, h3IdSample)
      .partition(_.is_new_pp)
    assert(newPps1.size == 1)
    assert(snappedPps1.isEmpty)
    assert(newPps1.forall(_.direction.contains(1)))

  }

  test("single point should not snap to existing pp if height diff is too big") {

    val idGenerator = new PpIdGenerator(0, h3IdSample)

    val newPpms = Seq(
      PpWithMapping(0, 0, 0, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
    )
    val existingPps = Seq(
      Pp.from(newPpms.head, h3IdSample, idGenerator.nextPpId, srcCrs)
        .copy(z=10)
    )
    val (newPps1, snappedPps1) = transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(newPpms, existingPps, h3IdSample)
      .partition(_.is_new_pp)
    assert(newPps1.size == 1)
    assert(snappedPps1.isEmpty)
    assert(newPps1.forall(_.direction.contains(1)))

  }

  test("Create pps from square, respect priority, no new points when repeating") {

    // A square, each side one edge, with ascending priority.
    // Corner points are present in two edges and must be consolidated to one Pp by the snap algorithm.
    val squarePps = Seq(
      PpWithMapping(-0.5, -0.5, 100, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
      PpWithMapping(-0.25, -0.5, 100, 3, position = 0.25, uuid_edge = "1", prio = 1, azimuth = 0.0f),
      PpWithMapping(0, -0.5, 100, 3, position = 0.5, uuid_edge = "1", prio = 1, azimuth = 0.0f),
      PpWithMapping(0.25, -0.5, 100, 3, position = 0.75, uuid_edge = "1", prio = 1, azimuth = 0.0f),
      PpWithMapping(0.5, -0.5, 100, 2, position = 1.0, uuid_edge = "1", prio = 1, azimuth = 0.7853982f),
      PpWithMapping(0.5, -0.5, 100, 2, position = 1.0, uuid_edge = "2", prio = 2, azimuth = 0.7853982f),
      PpWithMapping(0.5, -0.25, 102.5f, 3, position = 1.25, uuid_edge = "2", prio = 2, azimuth = 1.5707964f),
      PpWithMapping(0.5, 0, 105, 3, position = 1.5, uuid_edge = "2", prio = 2, azimuth = 1.5707964f),
      PpWithMapping(0.5, 0.25, 107.5f, 3, position = 1.75, uuid_edge = "2", prio = 2, azimuth = 1.5707964f),
      PpWithMapping(0.5, 0.5, 110, 2, position = 2.0, uuid_edge = "2", prio = 2, azimuth = 2.3561945f),
      PpWithMapping(0.5, 0.5, 110, 2, position = 2.0, uuid_edge = "3", prio = 3, azimuth = 2.3561945f),
      PpWithMapping(0.25, 0.5, 107.5f, 3, position = 2.25, uuid_edge = "3", prio = 3, azimuth = 3.1415927f),
      PpWithMapping(0, 0.5, 105, 3, position = 2.5, uuid_edge = "3", prio = 3, azimuth = 3.1415927f),
      PpWithMapping(-0.25, 0.5, 102.5f, 3, position = 2.75, uuid_edge = "3", prio = 3, azimuth = 3.1415927f),
      PpWithMapping(-0.5, 0.5, 100, 2, position = 3, uuid_edge = "3", prio = 3, azimuth = -2.3561945f),
      PpWithMapping(-0.5, 0.5, 100, 2, position = 3, uuid_edge = "4", prio = 4, azimuth = -2.3561945f),
      PpWithMapping(-0.5, 0.25, 100, 3, position = 3.25, uuid_edge = "4", prio = 4, azimuth = -1.5707964f),
      PpWithMapping(-0.5, 0, 100, 3, position = 3.5, uuid_edge = "4", prio = 4, azimuth = -1.5707964f),
      PpWithMapping(-0.5, -0.25, 100, 3, position = 3.75, uuid_edge = "4", prio = 4, azimuth = -1.5707964f),
      PpWithMapping(-0.5, -0.5, 100, 0, position = 4.0, uuid_edge = "4", prio = 4, azimuth = -1.5707964f),
    )

    // first snap -> create new pps
    println("FIRST")
    var existingPps = Seq[Pp]()
    val (newPps1, snappedPps1) = transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(squarePps, existingPps, h3IdSample)
      .partition(_.is_new_pp)
    assert(newPps1.size == squarePps.size - 4)
    assert(snappedPps1.size == 4)
    assert(newPps1.forall(_.direction.contains(1)))

    // second snap -> snap to existing pps
    println("SECOND")
    existingPps = existingPps ++ newPps1.map(_.snapped_pp)
    val (newPps2, snappedPps2) = transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(squarePps, existingPps, h3IdSample)
      .partition(_.is_new_pp)
    assert(newPps2.isEmpty)
    assert(snappedPps2.size == squarePps.size)

  }

  test("Create pps for switch, respect main track priority, no new points when repeating") {

    val edge1 = Seq(
      PpWithMapping(0, 0, 0, 0.toShort, 0.0, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 0.25, 0, 0.toShort, 0.25, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 0.5, 0, 0.toShort, 0.5, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 0.75, 0, 0.toShort, 0.75, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 1.0, 0, 0.toShort, 1.0, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 1.25, 0, 0.toShort, 1.25, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 1.5, 0, 0.toShort, 1.5, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 1.75, 0, 0.toShort, 1.75, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 2.0, 0, 0.toShort, 2.0, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 2.25, 0, 0.toShort, 2.25, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 2.5, 0, 0.toShort, 2.5, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 2.75, 0, 0.toShort, 2.75, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 3.0, 0, 0.toShort, 3.0, "1", 1.toShort, azimuth = 0.0f)
    )

    val edge2 = Seq(
      PpWithMapping(0.0, 0.0, 0, 0.toShort, 0.0, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.08, 0.24, 0, 0.toShort, 0.25, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.16, 0.47, 0, 0.toShort, 0.5, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.24, 0.71, 0, 0.toShort, 0.75, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.32, 0.95, 0, 0.toShort, 1.0, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.4, 1.19, 0, 0.toShort, 1.25, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.47, 1.42, 0, 0.toShort, 1.5, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.55, 1.66, 0, 0.toShort, 1.75, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.63, 1.9, 0, 0.toShort, 2.0, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.71, 2.13, 0, 0.toShort, 2.25, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.79, 2.37, 0, 0.toShort, 2.5, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.87, 2.61, 0, 0.toShort, 2.75, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.95, 2.85, 0, 0.toShort, 3.0, "2", 1.toShort, azimuth = 0.0f),
    )

    // first snap -> create new pps
    println("FIRST")
    var existingPps = Seq[Pp]()
    val (newPps1, snappedPps1) = transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(edge1 ++ edge2, existingPps, h3IdSample)
      .partition(_.is_new_pp)
    assert(newPps1.size == 22)
    assert(snappedPps1.size == 4)
    assert(snappedPps1.count(x => x.pp.get.getGeometry(lv95GeomFactory) != x.snapped_pp.getGeometry(lv95GeomFactory)) == 3) // Anzahl Punkte welche beim Snapping verschoben wurden...
    assert(snappedPps1.forall(_.direction.contains(1)))

    // second snap -> snap to existing pps
    println("SECOND")
    existingPps = existingPps ++ newPps1.map(_.snapped_pp)
    val (newPps2, snappedPps2) = transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(edge1 ++ edge2, existingPps, h3IdSample)
      .partition(_.is_new_pp)
    assert(newPps2.isEmpty)
    assert(snappedPps2.size == 26)
    assert(snappedPps2.count(x => x.pp.get.getGeometry(lv95GeomFactory) != x.snapped_pp.getGeometry(lv95GeomFactory)) == 3) // Anzahl Punkte welche beim Snapping verschoben wurden...
    assert(snappedPps1.forall(_.direction.contains(1)))

  }

}
