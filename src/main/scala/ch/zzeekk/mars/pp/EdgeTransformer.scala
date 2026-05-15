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
import ch.zzeekk.mars.pp.utils.GeometryCalcUtils.isMetricCrs
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.locationtech.jts.geom.{Geometry, LineString, Point}

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Build edges of the railway network.
 * Tracks that have been splitted need to be combined again.
 * They are splitted for example because of changing attributes, e.g. beginning of bridges or tunnels.
 */
class EdgeTransformer extends CustomDfsTransformer {

  def transform(dsTrack: Dataset[Track], isExec: Boolean, srcCrs: String, tagsToIgnore: Seq[String] = Seq()): Dataset[Edge] = {
    implicit val session: SparkSession = dsTrack.sparkSession
    assert(isMetricCrs(srcCrs), "CRS must be metric for distance calculations; got " + srcCrs)

    import session.implicits._

    // extract tracks
    // filter tracks with at least 2 points, otherwise they make no sense
    val tracks = if(isExec) {
      dsTrack
        .where(ST_NumPoints($"geometry") > 1)
        .collect().toSeq
    } else Seq()

    if (isExec) logger.info(s"merging #${tracks.size} tracks to edges...")
    def isCircular(t: Track): Boolean = t.linestring.isRing
    if (isExec) logger.warn(s"excluding #${tracks.count(isCircular)} circular tracks...")
    val edges = mergeTracks(tracks.filterNot(isCircular), tagsToIgnore.toSet)

    // enrich node uuids
    val nodeUuids = (edges.map(_.startPointAndId) ++ edges.map(_.endPointAndId))
      .map{ case (p, id) => (p, createUuid(id.getOrElse(p.toString))) }.toMap
    val edgeEnriched = edges
      .map(e => Edge(
        createUuid(e.tracks.map(_.uuid_track).mkString("|")),
        e.geometry,
        e.tracks,
        uuid_node_from = nodeUuids(e.linestring.getStartPoint),
        uuid_node_to = nodeUuids(e.linestring.getEndPoint),
        e.tags,
        src_id_node_from = e.src_id_node_from,
        src_id_node_to = e.src_id_node_to
      ))

    // check for circular edges which create problems later
    val circularEdges = edgeEnriched.filter(e => e.uuid_node_from == e.uuid_node_to)
    assert(circularEdges.isEmpty, s"Circular edges detected: ${circularEdges.map(_.tracks.map(_.uuid_track).mkString("|")).mkString(", ")}")

    if (isExec) logger.info(s"got #${edgeEnriched.size} edges after merging tracks")
    edgeEnriched.toDS()
  }

  def mergeTracks(tracks: Seq[Track], tagsToIgnore: Set[String]): Seq[EdgePrep] = {
    val tracksBidir = tracks ++ tracks.map(_.reverse)
    val tracksByPoint = tracksBidir
      .groupBy(_.linestring.getStartPoint)
      .view.mapValues(_.toSet)
      .filter { case (_,v) =>
        val normalizedTags = v.map(_.tags.diff(tagsToIgnore))
        v.size == 2 && // splitted tracks
          normalizedTags.toSeq.distinct.length == 1 // with same effective tags
      }.toMap

    // define recursion to combine with next track
    @tailrec
    def combineNext(edge: EdgePrep): EdgePrep = {
      val next = tracksByPoint.get(edge.linestring.getEndPoint)
      if (next.isDefined) {
        val nextTrack = next.get.filter(_.uuid_track != edge.tracks.last.uuid_track)
        assert(nextTrack.size==1, s"Next track not found for ${edge.tracks.last.uuid_track}. Try to exclude it from merge by setting option trackUuidsToExcludeFromMerge. ($next)")
        // avoid circles
        if (nextTrack.head.linestring.getEndPoint != edge.linestring.getStartPoint) {
          combineNext(edge.add(nextTrack.head, tagsToIgnore))
        } else edge
      } else edge
    }

    // create edges
    // start with tracks where startPoint has no entry in tracksByPoint and combine with following tracks if possible
    val startTracks = tracksBidir
      .filter(t => !tracksByPoint.contains(t.linestring.getStartPoint))
    val mergedGeoms = mutable.Buffer[EdgePrep]()
    val skipTrackUuids = mutable.HashSet[String]()
    startTracks.foreach { nextTrack =>
      if (!skipTrackUuids.contains(nextTrack.uuid_track)) {
        val mergedGeom = combineNext(EdgePrep.from(nextTrack, tagsToIgnore))
        mergedGeoms.append(mergedGeom)
        // Skip the opposite chain start in reverse direction.
        skipTrackUuids.add(mergedGeom.tracks.last.uuid_track)
      }
    }
    mergedGeoms.toSeq
  }

  def createUuid(uniqueStr: String): String = {
    uniqueStr match {
      case longStr if longStr.toLongOption.isDefined => new UUID(0L, longStr.toLong)
      case str => UUID.nameUUIDFromBytes(str.getBytes())
    }
  }.toString

}

case class Edge(uuid_edge: String, geometry: Geometry, tracks: Seq[TrackRef], uuid_node_from: String, uuid_node_to: String, tags: Set[String], src_id_node_from: Option[String], src_id_node_to: Option[String])

case class EdgePrep(geometry: Geometry, tracks: Seq[TrackRef], tags: Set[String], src_id_node_from: Option[String], src_id_node_to: Option[String]) {
  def linestring: LineString = geometry.asInstanceOf[LineString]
  def add(track: Track, tagsToIgnore: Set[String]): EdgePrep = {
    val lastPosition = tracks.lastOption.map(_.position_to).getOrElse(0d)
    copy(
      geometry = GeometryCalcUtils.mergeLineStrings(linestring, track.linestring),
      tracks = tracks :+ TrackRef(track.uuid_track, lastPosition, lastPosition + track.length, if(track.reversed) -1 else 1),
      src_id_node_to = track.src_id_node_to,
      tags = tags ++ track.tags.diff(tagsToIgnore)
    )
  }
  def startPointAndId: (Point, Option[String]) = (linestring.getStartPoint, src_id_node_from)
  def endPointAndId: (Point, Option[String]) = (linestring.getEndPoint, src_id_node_to)
}
object EdgePrep {
  def from(track: Track, tagsToIgnore: Set[String]): EdgePrep = {
    EdgePrep(geometry = track.geometry, tracks = Seq(TrackRef.from(track)), tags = track.tags.diff(tagsToIgnore), src_id_node_from = track.src_id_node_from, src_id_node_to = track.src_id_node_to)
  }
}

case class TrackRef(uuid_track: String, position_from: Double, position_to: Double, direction: Short)
object TrackRef {
  def from(track: Track): TrackRef = {
    TrackRef(track.uuid_track, 0d, track.geometry.getLength, if (track.reversed) -1 else 1)
  }
}

case class Track(uuid_track: String, src_id: Option[String], src_id_node_from: Option[String], src_id_node_to: Option[String], geometry: Geometry, reversed: Boolean, tags: Set[String], properties: Map[String,String]) {
  def linestring: LineString = geometry.asInstanceOf[LineString]
  def length: Double = geometry.getLength
  def reverse: Track = copy(src_id_node_from = src_id_node_to, src_id_node_to = src_id_node_from, geometry = geometry.reverse(), reversed = true)
}
