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
package ch.zzeekk.mars.de.isr

import ch.zzeekk.mars.pp.Track
import ch.zzeekk.mars.pp.utils.GeometryCalcUtils
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.locationtech.jts.geom.Geometry

/**
 * Creating standardized tracks for further processing.
 */
class IsrTrackTransformer extends CustomDfsTransformer {

  def transform(dfSlvDeIsrStreckenabschnitt: DataFrame): Dataset[Track] = {
    implicit val session: SparkSession = dfSlvDeIsrStreckenabschnitt.sparkSession
    import session.implicits._

    val udfSplitAcuteLineStrings = udf((geometry: Geometry) => GeometryCalcUtils.splitAcuteGeometry(geometry))

    val dsTrack = dfSlvDeIsrStreckenabschnitt
      .withColumn("geometry", explode(ST_Dump($"geometry"))) // explode MULTILINESTRING into LINESTRINGs
      .withColumn("geometry", explode(udfSplitAcuteLineStrings($"geometry"))) // split LINESTRINGs at acute angles into multiple LINESTRINGs
      .where(ST_NumPoints($"geometry") > 1)
      .select(
        $"uuid".as("uuid_track"),
        $"geometry",
        lit(false).as("reversed"),
        array_compact(array(
          when($"db_netz_strecke" === "DB", "DB"),
          when($"regelspurweite" === "1435", "Normalspur"),
          when($"traktionsart" === "Oberleitung", "Elektrifiziert"),
          when($"gleisanzahl" isin("Gegengleis", "Richtungsgleis"), "Doppelspurig")
        )).as("tags")
      ).as[Track]

    dsTrack
  }


}
