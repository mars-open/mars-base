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
import net.mars.open.pp.Switch
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Creating standardized switches for further processing.
 */
class OsmSwitchTransformer extends CustomDfsTransformer {

  def transform(dfSlvOsmSwitchRaw: DataFrame): Dataset[Switch] = {
    implicit val session: SparkSession = dfSlvOsmSwitchRaw.sparkSession
    import session.implicits._

    dfSlvOsmSwitchRaw
      .select(
        $"uuid".as("uuid_node"),
        $"id".cast("string").as("src_id"),
        $"geometry".as("geometry"),
        array_compact(array(
          lit("switch")
        )).as("tags"),
        map_filter(map(
          lit("op"), $"operator",
          lit("nb"), $"ref",
          lit("turnout_side"), $"turnout_side",
          lit("maxspeed_diverging"), $"maxspeed_diverging",
          lit("maxspeed_straight"), $"maxspeed_straight",
          lit("radius"), $"radius"
        ), (_, v) => length(v) > 0).as("properties")
      ).as[Switch]
  }
}
