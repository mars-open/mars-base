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
