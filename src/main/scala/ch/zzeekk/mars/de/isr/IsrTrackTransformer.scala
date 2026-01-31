package ch.zzeekk.mars.de.isr

import ch.zzeekk.mars.pp.Track
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Creating standardized tracks for further processing.
 */
class IsrTrackTransformer extends CustomDfsTransformer {

  def transform(dfSlvDeIsrStreckenabschnitt: DataFrame): Map[String, DataFrame] = {
    implicit val session: SparkSession = dfSlvDeIsrStreckenabschnitt.sparkSession
    import session.implicits._

    val dsTrack = dfSlvDeIsrStreckenabschnitt
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

    Map("track" -> dsTrack.toDF)
  }

}
