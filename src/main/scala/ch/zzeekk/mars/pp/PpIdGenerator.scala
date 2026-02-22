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
package ch.zzeekk.mars.pp

import ch.zzeekk.mars.pp.PpIdGenerator._
import ch.zzeekk.mars.pp.utils.GeometryCalcUtils.{convertTo4326, getGeoFactory}
import com.uber.h3core.H3Core
import org.apache.commons.codec.binary.Base32
import org.locationtech.jts.geom.Coordinate

import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.atomic.AtomicInteger


/**
 * Create Positionpoint IDs for a given h3 cell.
 * The ID is composed of
 * - base cell number and detail digits of h3 Cell id (52 bits)
 * - numbering of the Positionpoints within this cell (12 bits)
 */
class PpIdGenerator(cntExistingPps: Int, h3id: Long) {
  // check h3 Cell id type and resolution
  assert(h3.isValidCell(h3id), s"h3id is not a cell ($h3id)")
  private val resolution = h3.getResolution(h3id)
  assert(resolution == 15, s"h3id should have resolution 15, but is $resolution ($h3id)")

  private val nextPpNumberWithinCell = new AtomicInteger(cntExistingPps)

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
    val geoFactory = getGeoFactory(srcCrs)
    val wgs84coord = convertTo4326(geoFactory.createPoint(new Coordinate(x, y)), srcCrs).getCoordinate
    h3.latLngToCell(wgs84coord.getY, wgs84coord.getX, 15)
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