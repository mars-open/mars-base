package ch.zzeekk.mars.pp.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SparkUtils {

  def jsonifyComplexDataTypes(df: DataFrame): DataFrame = {
    val colNames = df.schema.filter(f => Seq("array", "map", "struct").contains(f.dataType.typeName)).map(_.name)
    colNames.foldLeft(df) {
      case (df, colName) => df.withColumn(colName, to_json(col(colName)))
    }
  }

}
