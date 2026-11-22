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
      PpWithMapping(0, 0, Some(0), Some(0), 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
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
      PpWithMapping(0, 0, Some(0), None, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
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
      PpWithMapping(0, 0, Some(0), None, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
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
      PpWithMapping(0, 0, Some(0), None, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
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
      PpWithMapping(0, 0, Some(0), None, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
    )
    val existingPps = Seq(
      Pp.from(newPpms.head, h3IdSample, idGenerator.nextPpId, srcCrs)
        .copy(z=Some(10))
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
      PpWithMapping(-0.5, -0.5, Some(100), None, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f),
      PpWithMapping(-0.25, -0.5, Some(100), None, 3, position = 0.25, uuid_edge = "1", prio = 1, azimuth = 0.0f),
      PpWithMapping(0, -0.5, Some(100), None, 3, position = 0.5, uuid_edge = "1", prio = 1, azimuth = 0.0f),
      PpWithMapping(0.25, -0.5, Some(100), None, 3, position = 0.75, uuid_edge = "1", prio = 1, azimuth = 0.0f),
      PpWithMapping(0.5, -0.5, Some(100), None, 2, position = 1.0, uuid_edge = "1", prio = 1, azimuth = 0.7853982f),
      PpWithMapping(0.5, -0.5, Some(100), None, 2, position = 1.0, uuid_edge = "2", prio = 2, azimuth = 0.7853982f),
      PpWithMapping(0.5, -0.25, Some(102.5f), None, 3, position = 1.25, uuid_edge = "2", prio = 2, azimuth = 1.5707964f),
      PpWithMapping(0.5, 0, Some(105), None, 3, position = 1.5, uuid_edge = "2", prio = 2, azimuth = 1.5707964f),
      PpWithMapping(0.5, 0.25, Some(107.5f), None, 3, position = 1.75, uuid_edge = "2", prio = 2, azimuth = 1.5707964f),
      PpWithMapping(0.5, 0.5, Some(110), None, 2, position = 2.0, uuid_edge = "2", prio = 2, azimuth = 2.3561945f),
      PpWithMapping(0.5, 0.5, Some(110), None, 2, position = 2.0, uuid_edge = "3", prio = 3, azimuth = 2.3561945f),
      PpWithMapping(0.25, 0.5, Some(107.5f), None, 3, position = 2.25, uuid_edge = "3", prio = 3, azimuth = 3.1415927f),
      PpWithMapping(0, 0.5, Some(105), None, 3, position = 2.5, uuid_edge = "3", prio = 3, azimuth = 3.1415927f),
      PpWithMapping(-0.25, 0.5, Some(102.5f), None, 3, position = 2.75, uuid_edge = "3", prio = 3, azimuth = 3.1415927f),
      PpWithMapping(-0.5, 0.5, Some(100), None, 2, position = 3, uuid_edge = "3", prio = 3, azimuth = -2.3561945f),
      PpWithMapping(-0.5, 0.5, Some(100), None, 2, position = 3, uuid_edge = "4", prio = 4, azimuth = -2.3561945f),
      PpWithMapping(-0.5, 0.25, Some(100), None, 3, position = 3.25, uuid_edge = "4", prio = 4, azimuth = -1.5707964f),
      PpWithMapping(-0.5, 0, Some(100), None, 3, position = 3.5, uuid_edge = "4", prio = 4, azimuth = -1.5707964f),
      PpWithMapping(-0.5, -0.25, Some(100), None, 3, position = 3.75, uuid_edge = "4", prio = 4, azimuth = -1.5707964f),
      PpWithMapping(-0.5, -0.5, Some(100), None, 0, position = 4.0, uuid_edge = "4", prio = 4, azimuth = -1.5707964f),
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
      PpWithMapping(0, 0, Some(0), None, 0.toShort, 0.0, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 0.25, Some(0), None, 0.toShort, 0.25, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 0.5, Some(0), None, 0.toShort, 0.5, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 0.75, Some(0), None, 0.toShort, 0.75, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 1.0, Some(0), None, 0.toShort, 1.0, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 1.25, Some(0), None, 0.toShort, 1.25, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 1.5, Some(0), None, 0.toShort, 1.5, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 1.75, Some(0), None, 0.toShort, 1.75, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 2.0, Some(0), None, 0.toShort, 2.0, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 2.25, Some(0), None, 0.toShort, 2.25, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 2.5, Some(0), None, 0.toShort, 2.5, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 2.75, Some(0), None, 0.toShort, 2.75, "1", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0, 3.0, Some(0), None, 0.toShort, 3.0, "1", 1.toShort, azimuth = 0.0f)
    )

    val edge2 = Seq(
      PpWithMapping(0.0, 0.0, Some(0), None, 0.toShort, 0.0, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.08, 0.24, Some(0), None, 0.toShort, 0.25, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.16, 0.47, Some(0), None, 0.toShort, 0.5, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.24, 0.71, Some(0), None, 0.toShort, 0.75, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.32, 0.95, Some(0), None, 0.toShort, 1.0, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.4, 1.19, Some(0), None, 0.toShort, 1.25, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.47, 1.42, Some(0), None, 0.toShort, 1.5, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.55, 1.66, Some(0), None, 0.toShort, 1.75, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.63, 1.9, Some(0), None, 0.toShort, 2.0, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.71, 2.13, Some(0), None, 0.toShort, 2.25, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.79, 2.37, Some(0), None, 0.toShort, 2.5, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.87, 2.61, Some(0), None, 0.toShort, 2.75, "2", 1.toShort, azimuth = 0.0f),
      PpWithMapping(0.95, 2.85, Some(0), None, 0.toShort, 3.0, "2", 1.toShort, azimuth = 0.0f),
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

  /**
   * Create Positionpoint candidates every 0.25m along a straight line.
   */
  def straightEdge(uuidEdge: String, x0: Double, y0: Double, angle: Double, nbOfPoints: Int, z: Float = 100f, prio: Short = 1): Seq[PpWithMapping] =
    (0 until nbOfPoints).map { i =>
      val pos = i * 0.25
      PpWithMapping(x0 + pos * Math.cos(angle), y0 + pos * Math.sin(angle), Some(z), None, 3, position = pos, uuid_edge = uuidEdge, prio = prio,
        azimuth = angle.toFloat, edge_idx = i)
    }

  def snap(newPpms: Seq[PpWithMapping], existingPps: Seq[Pp]): (Seq[PpWithMappingAndSnap], Seq[PpWithMappingAndSnap]) =
    transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(newPpms, existingPps, h3IdSample).partition(_.is_new_pp)

  def assertUniqueIds(existingPps: Seq[Pp], newPps: Seq[PpWithMappingAndSnap]): Unit = {
    val ids = existingPps.map(_.id_positionpoint) ++ newPps.map(_.snapped_pp.id_positionpoint)
    assert(ids.distinct.size == ids.size, "ids of new Positionpoints must not collide with existing ones")
  }

  test("new pp ids continue after highest existing number in cell, even with gaps and duplicates") {
    val ppm = PpWithMapping(0, 0, Some(0), None, 0, position = 0.0, uuid_edge = "1", prio = 1, azimuth = 0.0f)
    val pp0 = Pp.from(ppm, h3IdSample, new PpIdGenerator(0, h3IdSample).nextPpId, srcCrs).copy(x = 10)
    val pp5 = Pp.from(ppm, h3IdSample, new PpIdGenerator(5, h3IdSample).nextPpId, srcCrs).copy(x = 20)
    val existingPps = Seq(pp0, pp5, pp5, pp5) // duplicates and gap in numbering
    val (newPps, snappedPps) = snap(Seq(ppm), existingPps)
    assert(snappedPps.isEmpty)
    assert(newPps.size == 1)
    assert(PpIdGenerator.getPpNumber(newPps.head.snapped_pp.id_positionpoint) == 6)
    assertUniqueIds(existingPps.distinct, newPps)
  }

  test("repeating with unchanged candidates creates no new pps") {
    val edges = straightEdge("1", 0, 0, 0, 20) ++ straightEdge("2", 0, 3, 0, 20)
    val (newPps1, _) = snap(edges, Seq())
    assert(newPps1.size == 40)
    val existingPps = newPps1.map(_.snapped_pp)
    val (newPps2, snappedPps2) = snap(edges, existingPps)
    assert(newPps2.isEmpty)
    assert(snappedPps2.size == 40)
    // every candidate maps to the pp it created in the first run
    assert(snappedPps2.map(x => (x.pp, x.snapped_pp.id_positionpoint)).toSet == newPps1.map(x => (x.pp, x.snapped_pp.id_positionpoint)).toSet)
  }

  test("shifted track creates new pps, unchanged track not") {
    val edge1 = straightEdge("1", 0, 0, 0, 20)
    val edge2 = straightEdge("2", 0, 3, 0, 20)
    val (newPps1, _) = snap(edge1 ++ edge2, Seq())
    val existingPps = newPps1.map(_.snapped_pp)
    val edge2Shifted = straightEdge("2", 0, 3.5, 0, 20) // shifted by 0.5m > ppRadius
    val (newPps2, snappedPps2) = snap(edge1 ++ edge2Shifted, existingPps)
    assert(newPps2.size == 20)
    assert(newPps2.forall(_.pp.get.uuid_edge == "2"))
    assert(snappedPps2.size == 20)
    assert(snappedPps2.forall(_.pp.get.uuid_edge == "1"))
    assertUniqueIds(existingPps, newPps2)
  }

  test("track shifted less than ppRadius snaps to existing pps") {
    val edge1 = straightEdge("1", 0, 0, 0, 20)
    val (newPps1, _) = snap(edge1, Seq())
    val edge1Shifted = straightEdge("1", 0, 0.1, 0, 20)
    val (newPps2, snappedPps2) = snap(edge1Shifted, newPps1.map(_.snapped_pp))
    assert(newPps2.isEmpty)
    assert(snappedPps2.size == 20)
  }

  test("extended track creates new pps only for the extension") {
    val (newPps1, _) = snap(straightEdge("1", 0, 0, 0, 20), Seq())
    val existingPps = newPps1.map(_.snapped_pp)
    val (newPps2, snappedPps2) = snap(straightEdge("1", 0, 0, 0, 28), existingPps)
    assert(newPps2.size == 8)
    assert(newPps2.map(_.pp.get.edge_idx).toSet == (20 until 28).toSet)
    assert(snappedPps2.size == 20)
    assertUniqueIds(existingPps, newPps2)
  }

  test("height change above tolerance creates new pps, within tolerance not") {
    val (newPps1, _) = snap(straightEdge("1", 0, 0, 0, 20, z = 100f), Seq())
    val existingPps = newPps1.map(_.snapped_pp)
    // new bridge over existing track
    val (newPps2, snappedPps2) = snap(straightEdge("2", 0, 0, 0, 20, z = 105f), existingPps)
    assert(newPps2.size == 20)
    assert(snappedPps2.isEmpty)
    assertUniqueIds(existingPps, newPps2)
    // height correction within tolerance
    val (newPps3, snappedPps3) = snap(straightEdge("1", 0, 0, 0, 20, z = 100.5f), existingPps)
    assert(newPps3.isEmpty)
    assert(snappedPps3.size == 20)
  }

  test("new turnout at existing main track creates new pps only where turnout leaves the main track") {
    val main = straightEdge("1", 0, 0, Math.PI / 2, 13, z = 0f)
    val turnout = straightEdge("2", 0, 0, Math.atan2(3, 1), 13, z = 0f, prio = 2) // diverging ~18deg
    val (newPps1, _) = snap(main, Seq())
    val existingPps = newPps1.map(_.snapped_pp)
    val (newPps2, snappedPps2) = snap(main ++ turnout, existingPps)
    assert(newPps2.nonEmpty)
    assert(newPps2.forall(_.pp.get.uuid_edge == "2"))
    assert(snappedPps2.count(_.pp.get.uuid_edge == "2") > 0) // turnout points near the switch snap to the main track
    assert(snappedPps2.count(_.pp.get.uuid_edge == "1") == 13)
    assertUniqueIds(existingPps, newPps2)
    // and repeating creates nothing new
    val (newPps3, _) = snap(main ++ turnout, existingPps ++ newPps2.map(_.snapped_pp))
    assert(newPps3.isEmpty)
  }


  private def snapSingle(candidate: PpWithMapping, existingX: Double, existingY: Double): PpWithMappingAndSnap = {
    val existing = Pp.from(candidate, h3IdSample, new PpIdGenerator(0, h3IdSample).nextPpId, srcCrs).copy(x = existingX, y = existingY)
    val result = transformer.snapNewToExistingPps(ppRadius, ppHeightTolerance, srcCrs)(Seq(candidate), Seq(existing), h3IdSample)
    assert(result.size == 1)
    result.head
  }

  test("position of a new pp is the candidate position") {
    val candidate = PpWithMapping(0, 0, Some(0), None, 3, position = 10.0, uuid_edge = "1", prio = 1, azimuth = 0.0f)
    val result = snapSingle(candidate, 1, 1) // existing pp out of range
    assert(result.is_new_pp)
    assert(result.position == 10.0)
  }

  test("position is adapted by the offset along the edge when snapped to an existing pp") {
    // edge in x direction: offset along the edge is dx, the lateral offset dy is ignored
    val candidateX = PpWithMapping(0, 0, Some(0), None, 3, position = 10.0, uuid_edge = "1", prio = 1, azimuth = 0.0f)
    val resultX = snapSingle(candidateX, 0.1, 0.05)
    assert(!resultX.is_new_pp)
    assert(math.abs(resultX.position - 10.1) < 1e-6)
    assert(resultX.pp.get.position == 10.0) // the candidate itself is not changed

    // edge in y direction: offset along the edge is dy
    val candidateY = candidateX.copy(azimuth = (Math.PI / 2).toFloat)
    val resultY = snapSingle(candidateY, 0.05, -0.2)
    assert(!resultY.is_new_pp)
    assert(math.abs(resultY.position - 9.8) < 1e-6)

    // edge in reverse x direction: offset along the edge is -dx
    val candidateRev = candidateX.copy(azimuth = Math.PI.toFloat)
    val resultRev = snapSingle(candidateRev, 0.1, 0.0)
    assert(!resultRev.is_new_pp)
    assert(math.abs(resultRev.position - 9.9) < 1e-6)

    // diagonal edge (45°): existing pp 0.1m along the edge
    val candidateDiag = candidateX.copy(azimuth = (Math.PI / 4).toFloat)
    val d = 0.1 / math.sqrt(2)
    val resultDiag = snapSingle(candidateDiag, d, d)
    assert(math.abs(resultDiag.position - 10.1) < 1e-6)
  }

  test("position is not negative when snapped to an existing pp before the start of the edge") {
    val candidate = PpWithMapping(0, 0, Some(0), None, 3, position = 0.125, uuid_edge = "1", prio = 1, azimuth = 0.0f)
    val result = snapSingle(candidate, -0.2, 0)
    assert(!result.is_new_pp)
    assert(result.position == 0.0)
  }

  test("envelope of candidates is converted to EPSG:4326 with buffer") {
    import org.locationtech.jts.geom.{Coordinate, Envelope}
    import net.mars.open.pp.utils.GeometryCalcUtils._
    val envelope = new Envelope(2658000, 2708000, 1223000, 1273000)
    val bbox = transformer.getEnvelope4326WithBuffer(envelope, 0.5, srcCrs)
    // all points on the boundary of the buffered envelope must be inside the converted envelope
    val buffered = new Envelope(envelope); buffered.expandBy(0.5)
    val boundary = for (i <- 0 to 100; f = i / 100.0; c <- Seq(
      new Coordinate(buffered.getMinX + f * buffered.getWidth, buffered.getMinY), new Coordinate(buffered.getMinX + f * buffered.getWidth, buffered.getMaxY),
      new Coordinate(buffered.getMinX, buffered.getMinY + f * buffered.getHeight), new Coordinate(buffered.getMaxX, buffered.getMinY + f * buffered.getHeight)
    )) yield convertTo4326(lv95GeomFactory.createPoint(c), srcCrs).getCoordinate
    assert(boundary.forall(c => bbox.contains(c)))
    // Zürich region: lng ~8.3-9.0, lat ~47.1-47.6
    assert(bbox.getMinX > 8 && bbox.getMaxX < 9.2 && bbox.getMinY > 47 && bbox.getMaxY < 47.8, bbox.toString)
  }
}
