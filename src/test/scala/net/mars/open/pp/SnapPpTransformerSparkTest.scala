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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests SnapPpTransformer.transform with Spark on small data spread over many H3 cells,
 * focusing on repeated (incremental) runs against the Positionpoints created by previous runs.
 */
class SnapPpTransformerSparkTest extends AnyFunSuite with BeforeAndAfterAll {

  implicit lazy val session: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("SnapPpTransformerSparkTest")
    .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
  import session.implicits._

  override def afterAll(): Unit = session.stop()

  private val transformer = new SnapPpTransformer()
  private val srcCrs = "EPSG:2056"
  private val (x0, y0) = (2600000.0, 1200000.0) // Bern

  /** Create Positionpoint candidates every 0.25m along a straight line in LV95. */
  private def straightEdge(uuidEdge: String, x: Double, y: Double, angle: Double, nbOfPoints: Int, prio: Short, idxOffset: Int = 0): Seq[PpWithMapping] =
    (0 until nbOfPoints).map { i =>
      val pos = (i + idxOffset) * 0.25
      PpWithMapping(x + pos * Math.cos(angle), y + pos * Math.sin(angle), Some(550f), None, 3, position = pos, uuid_edge = uuidEdge, prio = prio,
        azimuth = angle.toFloat, edge_idx = i + idxOffset)
    }

  // main track 40m east, turnout diverging ~6deg (1:9) over 30m
  private val turnoutAngle = Math.atan2(1, 9)
  private val mainTrack = straightEdge("main", x0, y0, 0, 161, prio = 1)
  private val turnout = straightEdge("turnout", x0, y0, turnoutAngle, 121, prio = 2)

  private def asNullable(df: DataFrame): DataFrame =
    session.createDataFrame(df.rdd, StructType(df.schema.fields.map(_.copy(nullable = true))))

  case class Result(pps: Seq[Pp], mapping: Seq[(Long, String, Int)])

  private def run(candidates: Seq[PpWithMapping], existing: Seq[Pp]): Result = {
    // like tables read from Delta, all columns are nullable; spread input over partitions
    val dsNew: Dataset[PpWithMapping] = asNullable(candidates.toDS().toDF()).as[PpWithMapping].repartition(3)
    val dsExisting: Dataset[Pp] = asNullable(existing.toDS().toDF()).as[Pp].repartition(2)
    val result = transformer.transform(dsExisting, dsNew, nbOfSparkPartitions = 4, srcCrs = srcCrs, isExec = true)
    val pps = result("slv-pp").as[Pp].collect().toSeq
    val mapping = result("slv-pp-mapping").select($"id_positionpoint", $"uuid_edge", $"edge_idx").as[(Long, String, Int)].collect().toSeq
    Result(pps, mapping)
  }

  private def assertConsistent(existing: Seq[Pp], result: Result, candidates: Seq[PpWithMapping]): Unit = {
    val ids = existing.map(_.id_positionpoint) ++ result.pps.map(_.id_positionpoint)
    assert(ids.distinct.size == ids.size, "Positionpoint ids must be unique")
    assert(result.mapping.size == candidates.size, "every candidate must be mapped exactly once")
    assert(result.mapping.map(m => (m._2, m._3)).toSet == candidates.map(c => (c.uuid_edge, c.edge_idx)).toSet)
    assert(result.mapping.map(_._1).toSet.subsetOf(ids.toSet), "mapping must reference existing or new Positionpoints only")
  }

  test("test data spans many h3 cells") {
    val cells = (mainTrack ++ turnout).map(c => PpIdGenerator.getH3idL15(c.x, c.y, srcCrs)).distinct
    assert(cells.size > 20)
  }

  test("repeated runs create no new pps, changes create new pps") {
    val candidates1 = mainTrack ++ turnout

    // run 1: initial load
    val run1 = run(candidates1, Seq())
    assert(run1.pps.nonEmpty)
    assert(run1.pps.size < candidates1.size) // turnout points near the switch snap to the main track
    assertConsistent(Seq(), run1, candidates1)
    val existing1 = run1.pps

    // run 2: identical input -> no new pps
    val run2 = run(candidates1, existing1)
    assert(run2.pps.isEmpty, s"${run2.pps.size} new Positionpoints created when repeating with identical input")
    assertConsistent(existing1, run2, candidates1)

    // run 3: main track extended by 10m, and a section of the turnout moved 1m north
    val extension = straightEdge("main", x0, y0, 0, 40, prio = 1, idxOffset = 161)
    val (turnoutMoved, turnoutUnchanged) = turnout.partition(c => c.edge_idx >= 60 && c.edge_idx < 80)
    val turnoutShifted = turnoutMoved.map(c => c.copy(y = c.y + 1))
    val candidates3 = mainTrack ++ extension ++ turnoutUnchanged ++ turnoutShifted
    val run3 = run(candidates3, existing1)
    assertConsistent(existing1, run3, candidates3)
    assert(run3.pps.size == extension.size + turnoutShifted.size)
    // unchanged candidates keep their mapping
    val changedKeys = (extension ++ turnoutShifted).map(c => (c.uuid_edge, c.edge_idx)).toSet
    val run3Unchanged = run3.mapping.filterNot(m => changedKeys.contains((m._2, m._3))).toSet
    val run2Unchanged = run2.mapping.filterNot(m => changedKeys.contains((m._2, m._3))).toSet
    assert(run3Unchanged == run2Unchanged)
    // new pps are only mapped to changed candidates
    val newIds = run3.pps.map(_.id_positionpoint).toSet
    assert(run3.mapping.filter(m => newIds.contains(m._1)).map(m => (m._2, m._3)).toSet == changedKeys)

    // run 4: repeating changed input -> no new pps
    val existing3 = existing1 ++ run3.pps
    val run4 = run(candidates3, existing3)
    assert(run4.pps.isEmpty, s"${run4.pps.size} new Positionpoints created when repeating with changed input")
    assertConsistent(existing3, run4, candidates3)
  }

  /**
   * Find two points on a horizontal line, `dist` apart, that lie in neighbouring h3 cells (resolution 15).
   */
  private def pointsInNeighbourCells(dist: Double): ((Double, Double), (Double, Double)) = {
    val y = y0 + 100
    val x = Iterator.iterate(x0 + 100)(_ + 0.01)
      .find(x => PpIdGenerator.getH3idL15(x, y, srcCrs) != PpIdGenerator.getH3idL15(x + dist, y, srcCrs)).get
    ((x, y), (x + dist, y))
  }

  private def candidate(xy: (Double, Double), uuidEdge: String, prio: Short) =
    PpWithMapping(xy._1, xy._2, Some(550f), None, 3, position = 0, uuid_edge = uuidEdge, prio = prio, azimuth = 0f)

  test("candidate snaps to existing pp in neighbour cell") {
    val (xyExisting, xyNew) = pointsInNeighbourCells(0.1)
    val h3idExisting = PpIdGenerator.getH3idL15(xyExisting._1, xyExisting._2, srcCrs)
    assert(h3idExisting != PpIdGenerator.getH3idL15(xyNew._1, xyNew._2, srcCrs))
    val existingPp = Pp.from(candidate(xyExisting, "old", 1), h3idExisting, new PpIdGenerator(0, h3idExisting).nextPpId, srcCrs)
    val result = run(Seq(candidate(xyNew, "1", 1)), Seq(existingPp))
    assert(result.pps.isEmpty, "candidate must snap to existing pp in neighbour cell")
    assert(result.mapping.map(_._1) == Seq(existingPp.id_positionpoint))
  }

  test("candidate of next priority snaps to pp created in neighbour cell in previous round") {
    val (xyPrio1, xyPrio2) = pointsInNeighbourCells(0.1)
    val result = run(Seq(candidate(xyPrio1, "1", 1), candidate(xyPrio2, "2", 2)), Seq())
    assert(result.pps.size == 1)
    assert(result.mapping.map(_._1).toSet == Set(result.pps.head.id_positionpoint))
  }

  test("candidate does not snap to existing pp in neighbour cell out of range") {
    val (xyExisting, xyNew) = pointsInNeighbourCells(0.3)
    val h3idExisting = PpIdGenerator.getH3idL15(xyExisting._1, xyExisting._2, srcCrs)
    val existingPp = Pp.from(candidate(xyExisting, "old", 1), h3idExisting, new PpIdGenerator(0, h3idExisting).nextPpId, srcCrs)
    val result = run(Seq(candidate(xyNew, "1", 1)), Seq(existingPp))
    assert(result.pps.size == 1)
  }
}
