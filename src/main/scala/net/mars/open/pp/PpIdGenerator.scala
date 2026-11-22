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
package net.mars.open.pp

import PpIdGenerator._
import net.mars.open.pp.utils.GeometryCalcUtils.{convertTo4326, getGeoFactory}
import com.uber.h3core.H3Core
import org.apache.commons.codec.binary.Base32
import org.locationtech.jts.geom.Coordinate

import java.nio.{ByteBuffer, ByteOrder}
import scala.jdk.CollectionConverters._
import java.util.concurrent.atomic.AtomicInteger


/**
 * Create Positionpoint IDs for a given h3 cell.
 * The ID is composed of
 * - base cell number and detail digits of h3 Cell id (52 bits)
 * - numbering of the Positionpoints within this cell (12 bits)
 *
 * @param nextPpNumber the number within the cell to use for the next Positionpoint.
 *                     Use [[PpIdGenerator.nextPpNumber]] to derive it from existing Positionpoints,
 *                     so that numbers are never reused, even if there are gaps.
 */
class PpIdGenerator(nextPpNumber: Int, h3id: Long) {
  // check h3 Cell id type and resolution
  assert(h3.isValidCell(h3id), s"h3id is not a cell ($h3id)")
  private val resolution = h3.getResolution(h3id)
  assert(resolution == 15, s"h3id should have resolution 15, but is $resolution ($h3id)")

  private val nextPpNumberWithinCell = new AtomicInteger(nextPpNumber)

  def nextPpId: Long = {
    // max 12bit available for numbering; check for overflow.
    assert(nextPpNumberWithinCell.get() < (1 << 12), s"There are too many Positionspoints within h3 cell $h3id. Maximum is 12bits, e.g. < ${1 << 12}")
    // put h3 base and digit bits first, so PpId is sortable by h3 cell.
    (getBaseAndDigits(h3id) << 12) + nextPpNumberWithinCell.getAndIncrement()
  }
}

object PpIdGenerator {
  private val h3 = H3Core.newInstance

  /**
   * Get h3id of point for resolution 15.
   * Note: 15 is max h3 resolution
   */
  def getH3idL15(x: Double, y: Double, srcCrs: String): Long = {
    val resolution15 = 15
    val geoFactory = getGeoFactory(srcCrs)
    if (srcCrs != "EPSG:4326") {
      val wgs84coord = convertTo4326(geoFactory.createPoint(new Coordinate(x, y)), srcCrs).getCoordinate
      h3.latLngToCell(wgs84coord.getY, wgs84coord.getX, resolution15)
    } else h3.latLngToCell(y, x, resolution15)
  }

  private val ppNumberBits = 12

  /**
   * Extract the numbering of the Positionpoint within its h3 cell from a Positionpoint ID (lower 12 bits).
   */
  def getPpNumber(ppId: Long): Int = (ppId & ((1L << ppNumberBits) - 1)).toInt

  /**
   * Get the next free number within a h3 cell, given the IDs of the existing Positionpoints of this cell.
   * This is max + 1 and not the count, as existing numbers might have gaps.
   */
  def nextPpNumber(existingPpIds: Iterable[Long]): Int =
    if (existingPpIds.isEmpty) 0 else existingPpIds.map(getPpNumber).max + 1

  /**
   * Get the h3 cell (resolution 15) of a point and all neighbouring cells within the given distance.
   * This is much less than all 6 neighbours of the cell, as a res-15 cell has an edge length of ~0.58m.
   *
   * For every edge of the cell, the distance from the point to the edge is calculated. If it is in range,
   * the neighbour cell across this edge is included. In a hexagonal grid all cells touching a vertex of a cell
   * are also neighbours across one of its edges, so this is complete.
   * Distances are calculated with a local metric approximation of lat/lng degrees and a small margin (5%).
   *
   * @param lat latitude of the point (EPSG:4326)
   * @param lng longitude of the point (EPSG:4326)
   * @param h3id the h3 cell of the point (resolution 15)
   * @param distance distance in meters
   */
  def getH3idL15WithNeighboursInRange(lat: Double, lng: Double, h3id: Long, distance: Double): Seq[Long] = {
    val resolution15 = 15
    val maxDistance = distance * 1.05
    val metersPerDegreeLat = 111320d
    val metersPerDegreeLng = metersPerDegreeLat * Math.cos(Math.toRadians(lat))
    // cell boundary in local metric coordinates relative to the point
    val vertices = h3.cellToBoundary(h3id).asScala.map(v => ((v.lng - lng) * metersPerDegreeLng, (v.lat - lat) * metersPerDegreeLat)).toIndexedSeq
    val (cx, cy) = (vertices.map(_._1).sum / vertices.size, vertices.map(_._2).sum / vertices.size)
    val neighbours = vertices.indices.flatMap { i =>
      val (ax, ay) = vertices(i)
      val (bx, by) = vertices((i + 1) % vertices.size)
      if (distanceToSegment(ax, ay, bx, by) < maxDistance) {
        // a point slightly beyond the middle of the edge, seen from the cell center, is in the neighbour cell
        val (mx, my) = ((ax + bx) / 2, (ay + by) / 2)
        val (px, py) = (cx + (mx - cx) * 1.1, cy + (my - cy) * 1.1)
        Some(h3.latLngToCell(lat + py / metersPerDegreeLat, lng + px / metersPerDegreeLng, resolution15).longValue())
      } else None
    }
    (h3id +: neighbours).distinct
  }

  /** distance from the origin (0,0) to the segment a-b */
  private def distanceToSegment(ax: Double, ay: Double, bx: Double, by: Double): Double = {
    val (dx, dy) = (bx - ax, by - ay)
    val lengthSquared = dx * dx + dy * dy
    val t = if (lengthSquared == 0) 0d else Math.max(0d, Math.min(1d, -(ax * dx + ay * dy) / lengthSquared))
    Math.hypot(ax + t * dx, ay + t * dy)
  }

  /**
   * Extract base cell number and detail cell digits from h3 Cell ID.
   * This is implemented for cells with maximum possible resolution 15.
   * Details see https://h3geo.org/docs/core-library/h3Indexing
   */
  def getBaseAndDigits(h3id: Long): Long = {
    val baseAndDigits = h3id & 0xFFFFFFFFFFFFFL // base:7bits + digits:45bits, e.g. Bit 0-51 (inklusive)
    baseAndDigits
  }

  /**
   * Represent a Positionpoint Id as a compact String.
   * Base32 encoding is used for this (uppercase characters and numbers).
   *
   * @param ppId 64bit id of a positionpoint
   * @return String in format XXXX-XXXX-XXXXX using base32 encoding
   */
  def getToken(ppId: Long): String = {
    base32.encodeAsString(longToBytes(ppId))
      .take(13).patch(4, "-", 0).patch(9, "-", 0)
      .toLowerCase
  }

  private def longToBytes(value: Long): Array[Byte] = {
    ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array()
  }

  private val base32 = new Base32()
}