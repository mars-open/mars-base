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
