package ch.zzeekk.mars.ch.tlm3d

import ch.zzeekk.mars.pp.Track
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Creating standardized tracks for further processing.
 */
class Tlm3dTrackTransformer extends CustomDfsTransformer {

  def transform(dfSlvTlm3dTrack: DataFrame): Map[String, DataFrame] = {
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
          createTagFromBool("museumsbahn"),
          createTagFromBool("zahnradbahn"),
          createTagFromBool("standseilbahn"),
          createTagFromBool("betriebsbahn"),
          createTagFromBool("achse_dkm"),
        )).as("tags")
      ).as[Track]

    Map("track" -> dsTrack.toDF)
  }

}
