package ch.zzeekk.mars.pp

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils.convertCrs
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class CrsTransformer extends CustomDfTransformer {
  override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    val srcCrs = options("srcCrs")
    val tgtCrs = options("tgtCrs")
    val geometryColName = options.getOrElse("geometryColName", "geometry")
    val udfCrsTransform = udf(geometry => convertCrs(geometry, srcCrs, tgtCrs))
    df.withColumn(geometryColName, udfCrsTransform(col(geometryColName)))
  }
}
