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
package ch.zzeekk.mars.osm

import ch.zzeekk.mars.pp.Track
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * Creating standardized tracks for further processing.
 */
class OsmTrackTransformer extends CustomDfsTransformer {

  def transform(dfSlvOsmTrack: DataFrame): Map[String, DataFrame] = {
    implicit val session: SparkSession = dfSlvOsmTrack.sparkSession

    import session.implicits._

    def createTagFromBool(name: String) = when(col(name), lit(name))
    def createTagWithPrefixFromNumber(col: Column, prefix: String) = when(col.isNotNull, concat(lit(prefix),col))

    val dsTrack = dfSlvOsmTrack
      .where(ST_NumPoints($"geometry") > 1)
      .select(
        $"uuid".as("uuid_track"),
        $"geometry",
        lit(false).as("reversed"),
        array_compact(array(
          $"type", $"operator",
          createTagWithPrefixFromNumber($"ref", "line"),
          createTagWithPrefixFromNumber($"track_ref", "track"),
          createTagFromBool("main"),
        )).as("tags")
      ).as[Track]

    Map("track" -> dsTrack.toDF)
  }

}
