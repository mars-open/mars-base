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

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.locationtech.jts.geom.{Geometry, LineString}

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Build edges of the railway network.
 * Tracks that have been splitted need to be combined again.
 * They are splitted for example because of changing attributes, e.g. beginning of bridges or tunnels.
 */
class EdgeTransformer extends CustomDfsTransformer {

  def transform(dsTrack: Dataset[Track], isExec: Boolean, trackUuidsToExcludeFromMerge: Option[String] = None): Dataset[Edge] = {
    implicit val session: SparkSession = dsTrack.sparkSession

    import session.implicits._

    // extract tracks
    // filter tracks with at least 2 points, otherwise they make no sense
    val tracks = if(isExec) {
      dsTrack
        .where(ST_NumPoints($"geometry") > 1)
        //.where($"uuid_track".isin("b445d5de-5c44-4650-a2e6-e41217e5b22b","b914120c-be1e-402c-a816-9c43aa2745fa","fed9d4df-4019-4ca3-9e57-2b1a3bdea06a"))
        .collect().toSeq
    } else Seq()

    if (isExec) logger.info(s"merging #${tracks.size} tracks to edges...")
    val parsedTrackUuidsToExcludeFromMerge = trackUuidsToExcludeFromMerge
      .map(_.split(',').map(_.trim).toSet)
      .getOrElse(Set.empty[String])
    val edges = mergeTracks(tracks, parsedTrackUuidsToExcludeFromMerge)

    // enrich node uuids
    val nodeUuids = (edges.map(_.linestring.getStartPoint) ++ edges.map(_.linestring.getEndPoint))
      .map(p => (p, UUID.randomUUID().toString)).toMap
    val edgeEnriched = edges
      .map(e => Edge(
        e.uuid_edge, e.geometry, e.tracks,
        uuid_node_from = nodeUuids(e.linestring.getStartPoint),
        uuid_node_to = nodeUuids(e.linestring.getEndPoint),
        e.tags
      ))

    // check for circular edges which create problems later
    val circularEdges = edgeEnriched
      .filter(e => e.uuid_node_from == e.uuid_node_to)
    assert(circularEdges.isEmpty, s"""
      |Circular edges detected. Exclude them from merge by setting option trackUuidsToExcludeFromMerge, or remove them completely:
      |${circularEdges.map("  "+_.tracks.map(_.uuid_track).mkString("|")).mkString("\n")}
      |""".stripMargin
    )

    edgeEnriched.toDS()
  }

  def mergeTracks(tracks: Seq[Track], parsedTrackUuidsToExcludeFromMerge: Set[String]): Seq[EdgePrep] = {
    val tracksBidir = tracks ++ tracks.map(_.reverse)
    val tracksByPoint = tracksBidir
      .groupBy(_.linestring.getStartPoint)
      .view.mapValues(_.toSet)
      .filter { case (k,v) =>
        v.size == 2 && // splitted tracks
          v.map(_.tags).toSeq.distinct.length == 1 && // with same tags
          v.map(_.uuid_track).intersect(parsedTrackUuidsToExcludeFromMerge).isEmpty
      }.toMap

    // define recursion to combine with next track
    @tailrec
    def combineNext(edge: EdgePrep): EdgePrep = {
      val next = tracksByPoint.get(edge.linestring.getEndPoint)
      if (next.isDefined) {
        val nextTrack = next.get.filter(_.uuid_track != edge.tracks.last.uuid_track)
        assert(nextTrack.size==1, s"Next track not found for ${edge.tracks.last.uuid_track}. Try to exclude it from merge by setting option trackUuidsToExcludeFromMerge. ($next)")
        combineNext(edge.add(nextTrack.head))
      } else edge
    }

    // create edges
    // start with tracks where startPoint has no entry in tracksByPoint and combine with following tracks if possible
    val startTracks = tracksBidir
      .filter(t => !tracksByPoint.contains(t.linestring.getStartPoint))
      .toBuffer
    val mergedGeoms = mutable.Buffer[EdgePrep]()
    while (startTracks.nonEmpty) {
      val nextTrack = startTracks.head
      val mergedGeom = combineNext(EdgePrep.from(nextTrack))
      mergedGeoms.append(mergedGeom)
      startTracks.remove(0)
      val idx = startTracks.indexWhere(t => mergedGeom.tracks.last.uuid_track == t.uuid_track)
      if (idx >= 0) startTracks.remove(idx)
    }
    mergedGeoms.toSeq
  }

}

case class Edge(uuid_edge: String, geometry: Geometry, tracks: Seq[TrackRef], uuid_node_from: String, uuid_node_to: String, tags: Set[String])

case class EdgePrep(uuid_edge: String = UUID.randomUUID().toString, geometry: Geometry, tracks: Seq[TrackRef], tags: Set[String]) {
  def linestring: LineString = geometry.asInstanceOf[LineString]
  def add(track: Track): EdgePrep = {
    val lastPosition = tracks.lastOption.map(_.position_to).getOrElse(0d)
    copy(
      geometry = GeometryCalcUtils.mergeLineStrings(linestring, track.linestring),
      tracks = tracks :+ TrackRef(track.uuid_track, lastPosition, lastPosition + track.length, if(track.reversed) -1 else 1)
    )
  }
}
object EdgePrep {
  def from(track: Track): EdgePrep = {
    EdgePrep(geometry = track.geometry, tracks = Seq(TrackRef.from(track)), tags = track.tags)
  }
}

case class TrackRef(uuid_track: String, position_from: Double, position_to: Double, direction: Short)
object TrackRef {
  def from(track: Track): TrackRef = {
    TrackRef(track.uuid_track, 0d, track.geometry.getLength, if (track.reversed) -1 else 1)
  }
}

case class Track(uuid_track: String, geometry: Geometry, reversed: Boolean, tags: Set[String]) {
  def linestring: LineString = geometry.asInstanceOf[LineString]
  def length: Double = geometry.getLength
  def reverse: Track = copy(geometry = geometry.reverse(), reversed = true)
}
