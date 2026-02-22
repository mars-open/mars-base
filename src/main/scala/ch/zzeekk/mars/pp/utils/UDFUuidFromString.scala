package ch.zzeekk.mars.pp.utils

import io.smartdatalake.workflow.action.spark.customlogic.SparkUDFCreator
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.util.UUID

class UDFUuidFromString extends SparkUDFCreator {
  override def get(options: Map[String, String]): UserDefinedFunction = {
    udf((s: Option[String]) => s.map(s => UUID.nameUUIDFromBytes(s.getBytes("UTF-8")).toString))
  }
}