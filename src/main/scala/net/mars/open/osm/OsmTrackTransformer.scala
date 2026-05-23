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
package net.mars.open.osm

import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import net.mars.open.pp.Track
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.locationtech.jts.geom.{Geometry, GeometryFactory, LineString}

import java.util.UUID

/**
 * Creating standardized tracks for further processing.
 *
 * OSM tracks might not be split at switches, so this transformer splits tracks at switches.
 */
class OsmTrackTransformer extends CustomDfsTransformer {

  def transform(dfSlvOsmTrackRaw: DataFrame, dfSlvOsmSwitchRaw: DataFrame): Dataset[Track] = {
    implicit val session: SparkSession = dfSlvOsmTrackRaw.sparkSession
    import session.implicits._

    def createTagFromBool(name: String) = when(col(name), lit(name))

    val switchIds = dfSlvOsmSwitchRaw
      .select($"id".cast("long"))
      .as[Long]
      .collect()
      .toSet

    val udfSplitTrack = udf((geometry: Geometry, nodeIds: Seq[Long], id: Long) => OsmTrackTransformer.splitTrack(geometry, nodeIds, id, switchIds))
    val udfUuidFromLongs = udf((l1: Long, l2: Long) => new UUID(l1, l2).toString)

    dfSlvOsmTrackRaw
      .where(ST_NumPoints($"geometry") > 1)
      // split/explode tracks at switches, if necessary
      .select($"*", posexplode(udfSplitTrack($"geometry", $"node_ids", $"id")).as(Seq("split_idx", "track_split")))
      .select(
        when(ST_NumPoints($"geometry") === ST_NumPoints($"track_split.geometry"), $"uuid".cast("string"))
          .otherwise(udfUuidFromLongs($"split_idx", $"id"))
          .as("uuid_track"),
        concat($"id".cast("string"), lit(":"), $"split_idx".cast("string")).as("src_id"),
        $"track_split.nodeFrom".cast("string").as("src_id_node_from"),
        $"track_split.nodeTo".cast("string").as("src_id_node_to"),
        $"track_split.geometry".as("geometry"),
        lit(false).as("reversed"),
        $"level".cast("short").as("level"),
        array_compact(array(
          $"type",
          createTagFromBool("main"),
          createTagFromBool("bridge"),
          createTagFromBool("tunnel"),
        )).as("tags"),
        map_filter(map(
          lit("op"), $"operator",
          lit("line"), $"ref",
          lit("track"), coalesce($"track_ref", $"preferred_direction"),
          lit("speed"), $"maxspeed",
        ), (_, v) => length(v) > 0).as("properties")
      )
      // enrich level if not present, based on tags for bridge and tunnel
      .withColumn("level",
        when($"level".isNotNull, $"level")
          .when(array_contains($"tags", "bridge"), lit(1))
          .when(array_contains($"tags", "tunnel"), lit(-1))
          .otherwise(lit(0))
          .cast("short")
      )
      .as[Track]
  }
}

case class SplitTrackPart(geometry: Geometry, nodeFrom: Long, nodeTo: Long)

object OsmTrackTransformer {

  def splitTrack(geometry: Geometry, nodeIds: Seq[Long], id: Long, switchIds: Set[Long]): Seq[SplitTrackPart] = {
    assert(nodeIds.size == geometry.getNumPoints, s"Track $id: nodeIds.size (${nodeIds.size}) != geometry numPoints (${geometry.getNumPoints})")
    assert(geometry.isInstanceOf[LineString], s"Track $id: geometry must be LineString, got ${geometry.getGeometryType}")
    val line = geometry.asInstanceOf[LineString]

    val splitIndexes = nodeIds.zipWithIndex.collect {
      case (nodeId, idx) if idx > 0 && idx < nodeIds.size - 1 && switchIds.contains(nodeId) => idx
    }
    val splitBounds = (Seq(0) ++ splitIndexes ++ Seq(nodeIds.size - 1)).distinct.sorted

    splitBounds.sliding(2).collect {
      case Seq(fromIdx, toIdx) if toIdx > fromIdx =>
        SplitTrackPart(
          geometry = createSegment(line, fromIdx, toIdx),
          nodeFrom = nodeIds(fromIdx),
          nodeTo = nodeIds(toIdx)
        )
    }.toSeq
  }

  private def createSegment(line: LineString, fromIdx: Int, toIdx: Int): LineString = {
    val geometryFactory = new GeometryFactory(line.getPrecisionModel, line.getSRID)
    val coords = line.getCoordinates.slice(fromIdx, toIdx + 1).map(_.copy())
    geometryFactory.createLineString(coords)
  }
}
