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

import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import net.mars.open.pp.utils.GeometryCalcUtils
import net.mars.open.pp.utils.GeometryCalcUtils.isMetricCrs
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.locationtech.jts.geom.{Geometry, LineString, Point}

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Build edges of the railway network. Tracks that have been splitted need to be combined again.
 * They are splitted for example because of changing attributes, e.g. beginning of bridges or
 * tunnels.
 *
 * The transformer merges linear chains of compatible track segments into larger logical edges.
 * Compatibility is determined by topology (shared start/end points) and effective tags (after
 * applying `tagsToIgnore`).
 */
class EdgeTransformer extends CustomDfsTransformer {

  /**
   * Builds network edges from track fragments.
   *
   * @param dsTrack
   *   input track dataset
   * @param srcCrs
   *   CRS identifier of input geometries; must be metric for distance calculations
   * @param tagsToIgnore
   *   tags ignored during merge compatibility checks and edge tag aggregation
   * @return
   *   merged edges with deterministic node UUIDs and preserved source node ids
   */
  def transform(dsTrack: Dataset[Track], isExec: Boolean, srcCrs: String, tagsToIgnore: Seq[String] = Seq()): Dataset[Edge] = {
    implicit val session: SparkSession = dsTrack.sparkSession
    assert(isMetricCrs(srcCrs), "CRS must be metric for distance calculations; got " + srcCrs)

    import session.implicits._

    // extract tracks
    // filter tracks with at least 2 points, otherwise they make no sense
    val tracks = if (isExec) {
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
      .map { case (p, id) => (p, createUuid(id.getOrElse(p.toString))) }.toMap
    val edgeEnriched = edges
      .map(e =>
        Edge(
          createUuid(e.tracks.map(_.uuid_track).mkString("|")),
          e.geometry,
          e.tracks,
          uuid_node_from = nodeUuids(e.linestring.getStartPoint),
          uuid_node_to = nodeUuids(e.linestring.getEndPoint),
          e.tags,
          e.properties,
          src_id_node_from = e.src_id_node_from,
          src_id_node_to = e.src_id_node_to
        )
      )

    // check for circular edges which create problems later
    val circularEdges = edgeEnriched.filter(e => e.uuid_node_from == e.uuid_node_to)
    assert(circularEdges.isEmpty, s"Circular edges detected: ${circularEdges.map(_.tracks.map(_.uuid_track).mkString("|")).mkString(", ")}")

    if (isExec) logger.info(s"got #${edgeEnriched.size} edges after merging tracks")
    edgeEnriched.toDS()
  }

  /**
   * Merges topologically connected and tag-compatible tracks into edge candidates.
   *
   * The algorithm operates on bidirectional copies of tracks, starts chains at points where
   * deterministic continuation is not available, and recursively appends the unique compatible
   * continuation until no continuation exists.
   *
   * @param tracks
   *   input tracks to merge
   * @param tagsToIgnore
   *   tags ignored when checking compatibility between neighboring tracks
   * @return
   *   merged edge preparations used for final edge creation
   */
  def mergeTracks(tracks: Seq[Track], tagsToIgnore: Set[String]): Seq[EdgePrep] = {
    val tracksBidir = tracks ++ tracks.map(_.reverse)
    val tracksByPoint = tracksBidir
      .groupBy(_.linestring.getStartPoint)
      .view.mapValues(_.toSet)
      .filter { case (_, v) =>
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
        assert(
          nextTrack.size == 1,
          s"Next track not found for ${edge.tracks.last.uuid_track}. Try to exclude it from merge by setting option trackUuidsToExcludeFromMerge. ($next)"
        )
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

  /**
   * Creates a stable UUID for an arbitrary unique key.
   *
   * Numeric keys are mapped into the low bits of a UUID with a zero high part, while non-numeric
   * keys use name-based UUID generation.
   *
   * @param uniqueStr
   *   unique key string
   * @return
   *   deterministic UUID string
   */
  def createUuid(uniqueStr: String): String = {
    uniqueStr match {
      case longStr if longStr.toLongOption.isDefined => new UUID(0L, longStr.toLong)
      case str                                       => UUID.nameUUIDFromBytes(str.getBytes())
    }
  }.toString

}

/**
 * Final merged network edge.
 *
 * @param uuid_edge
 *   edge UUID
 * @param geometry
 *   merged edge geometry as LineString
 * @param tracks
 *   ordered source track references that build this edge
 * @param uuid_node_from
 *   UUID of the start node
 * @param uuid_node_to
 *   UUID of the end node
 * @param tags
 *   aggregated edge tags (after ignoring configured tags)
 * @param src_id_node_from
 *   optional source-system identifier of the start node
 * @param src_id_node_to
 *   optional source-system identifier of the end node
 */
case class Edge(
    uuid_edge: String,
    geometry: Geometry,
    tracks: Seq[TrackRef],
    uuid_node_from: String,
    uuid_node_to: String,
    tags: Set[String],
    properties: Map[String, String],
    src_id_node_from: Option[String],
    src_id_node_to: Option[String]
)

/**
 * Internal mutable-like representation of an edge.
 */
case class EdgePrep(
    geometry: Geometry,
    tracks: Seq[TrackRef],
    tags: Set[String],
    properties: Map[String, String],
    src_id_node_from: Option[String],
    src_id_node_to: Option[String]
) {
  def linestring: LineString = geometry.asInstanceOf[LineString]
  def combineProperties(p1: Map[String, String], p2: Map[String, String]): Map[String, String] = {
    // keep properties that have the same value in both maps
    p1.toSet.intersect(p2.toSet).toMap
  }
  def add(track: Track, tagsToIgnore: Set[String]): EdgePrep = {
    val lastPosition = tracks.lastOption.map(_.position_to).getOrElse(0d)
    copy(
      geometry = GeometryCalcUtils.mergeLineStrings(linestring, track.linestring),
      tracks = tracks :+ TrackRef.from(track, lastPosition),
      src_id_node_to = track.src_id_node_to,
      tags = tags ++ track.tags.diff(tagsToIgnore),
      properties = combineProperties(properties, track.properties)
    )
  }
  def startPointAndId: (Point, Option[String]) = (linestring.getStartPoint, src_id_node_from)
  def endPointAndId: (Point, Option[String]) = (linestring.getEndPoint,     src_id_node_to)
}
object EdgePrep {
  def from(track: Track, tagsToIgnore: Set[String]): EdgePrep =
    EdgePrep(
      geometry = track.geometry,
      tracks = Seq(TrackRef.from(track)),
      tags = track.tags.diff(tagsToIgnore),
      properties = track.properties,
      src_id_node_from = track.src_id_node_from,
      src_id_node_to = track.src_id_node_to
    )
}

/**
 * Reference to a source track inside a merged edge.
 *
 * @param uuid_track
 *   source track UUID
 * @param position_from
 *   begin position of this track along the merged edge
 * @param position_to
 *   end position of this track along the merged edge
 * @param direction
 *   orientation relative to the merged edge (+1 same, -1 reversed)
 * @param level
 *   optional vertical level (e.g. tunnel/bridge stacking level)
 * @param tags
 *   semantic tags used for matching/filtering/aggregation, including tunnel and bridge
 * @param properties
 *   additional source attributes as key-value pairs*, e.g. operator and line
 */
case class TrackRef(
    uuid_track: String,
    position_from: Double,
    position_to: Double,
    direction: Short,
    level: Option[Short],
    tags: Set[String],
    properties: Map[String, String]
)
object TrackRef {
  def from(track: Track, startPos: Double = 0d): TrackRef =
    TrackRef(track.uuid_track, startPos, startPos + track.length, if (track.reversed) -1 else 1, track.level, track.tags, track.properties)
}

/**
 * Standardized track segment used as edge-building input.
 *
 * @param uuid_track
 *   unique identifier of this track segment
 * @param src_id
 *   optional source-system identifier for the segment
 * @param src_id_node_from
 *   optional source-system identifier of the first node
 * @param src_id_node_to
 *   optional source-system identifier of the last node
 * @param geometry
 *   track geometry (expected to be a LineString)
 * @param reversed
 *   true when this segment was generated in reverse orientation
 * @param level
 *   optional vertical level (e.g. tunnel/bridge stacking level)
 * @param tags
 *   semantic tags used for matching/filtering/aggregation
 * @param properties
 *   additional source attributes as key-value pairs
 */
case class Track(
    uuid_track: String,
    src_id: Option[String],
    src_id_node_from: Option[String],
    src_id_node_to: Option[String],
    geometry: Geometry,
    reversed: Boolean,
    level: Option[Short],
    tags: Set[String],
    properties: Map[String, String]
) {
  def linestring: LineString = geometry.asInstanceOf[LineString]
  def length: Double = geometry.getLength
  def reverse: Track = copy(src_id_node_from = src_id_node_to, src_id_node_to = src_id_node_from, geometry = geometry.reverse(), reversed = true)
}
