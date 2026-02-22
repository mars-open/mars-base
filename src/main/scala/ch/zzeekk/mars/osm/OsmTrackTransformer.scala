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
