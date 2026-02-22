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
package ch.zzeekk.mars.ch.tlm3d

import ch.zzeekk.mars.pp.Track
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Creating standardized tracks for further processing.
 */
class Tlm3dEdgeInputTransformer extends CustomDfsTransformer {

  def transform(dfSlvTlm3dTrack: DataFrame): Dataset[Track] = {
    implicit val session: SparkSession = dfSlvTlm3dTrack.sparkSession

    import session.implicits._

    def createTagFromBool(name: String) = when(col(name), lit(name))

    val dsTrack = dfSlvTlm3dTrack
      .where(ST_NumPoints($"geometry") > 1)
      .select(
        $"uuid".as("uuid_track"),
        $"geometry",
        lit(false).as("reversed"),
        array_compact(array(
          $"type", $"subtype",
          createTagFromBool("main"),
          createTagFromBool("museumsbahn"),
          createTagFromBool("zahnradbahn"),
          createTagFromBool("standseilbahn"),
          createTagFromBool("betriebsbahn"),
          createTagFromBool("achse_dkm"),
        )).as("tags")
      ).as[Track]

    dsTrack
  }

}
