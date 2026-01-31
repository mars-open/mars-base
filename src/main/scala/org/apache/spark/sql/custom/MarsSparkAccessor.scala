package org.apache.spark.sql.custom

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.arrow.ArrowBatchStreamWriter
import org.apache.spark.sql.execution.arrow.ArrowConverters.ArrowBatchIterator
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, Row}

import java.io.OutputStream
import scala.jdk.CollectionConverters.IteratorHasAsScala

object MarsSparkAccessor {
  def createArrowBatchStreamWriter(schema: StructType,
                                   out: OutputStream,
                                   timeZoneId: String,
                                   errorOnDuplicatedFieldNames: Boolean): ArrowBatchStreamWriter = {
    new ArrowBatchStreamWriter(schema, out, timeZoneId, errorOnDuplicatedFieldNames)
  }

  def dfToArrowBatchIterator(df: DataFrame,
                             maxRecordsPerBatch: Long,
                             timeZoneId: String
                            ): ArrowBatchIterator = {
    val encoder = Encoders.row(df.schema).asInstanceOf[ExpressionEncoder[Row]].createSerializer()
    val internalRows = df.toLocalIterator().asScala.map(encoder.apply)
    new ArrowBatchIterator(internalRows, df.schema, maxRecordsPerBatch, timeZoneId, errorOnDuplicatedFieldNames = true, context = null)
  }
}
