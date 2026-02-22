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

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.locationtech.jts.index.strtree.STRtree

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Index of Objects with Geometry for fast querying.
 * @param initialData Objects with geometry the index should be initialized with.
 *                    Note that it is possible to add additional geometries later,
 *                    but before the next query the index will be reorganized by the current STRtree implementation.
 * @param geometryFn A function to extract the geometry from an object.
 * @tparam X the type of the objects
 */
class SpatialIndex[X](initialData: Seq[X], geometryFn: X => Geometry)(implicit factory: GeometryFactory) extends SmartDataLakeLogger {
  private var index: Option[STRtree] = None // note that STRtree needs to be rebuilt after querying before inserting new nodes. For good performance, insert and query operations should therefore not be mixed to often.
  private val data = Option(initialData).map(_.toBuffer).getOrElse(mutable.Buffer[X]())

  def count(pred: X => Boolean): Int = data.count(pred)

  def insert(element: X): Unit = {
    index = None // reset index
    data.append(element)
  }

  def insert(elements: Iterable[X]): Unit = {
    if (elements.nonEmpty) {
      index = None // reset index
      data.appendAll(elements)
    }
  }

  private def buildIndex(): Unit = {
    if (logger.isDebugEnabled) logger.debug(s"buildIndex data.size=${data.size}")
    val index = new STRtree()
    data.foreach( e => index.insert(geometryFn(e).getEnvelopeInternal, e))
    index.build()
    this.index = Some(index)
  }

  def queryInRangeOf(geometry: Geometry, distance: Float): Seq[X] = {
    if (index.isEmpty) buildIndex()
    val envelope = geometry.getEnvelopeInternal
    envelope.expandBy(distance)
    val objects = index.get.query(envelope).asInstanceOf[java.util.List[X]]
    objects.asScala.toSeq
      .filter(obj => geometryFn(obj).distance(geometry) < distance)
      .sortBy(obj => geometryFn(obj).distance(geometry)) // if multiple matches, sort ascending  by distance
  }
}
