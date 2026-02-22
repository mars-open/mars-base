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

object SeqUtils {

  def withPrevAndNext[I,R](coords: Seq[I])(fn: (Option[I],I,Option[I]) => R): Seq[R] = {
    coords
      .zip(None +: coords.init.map(Some(_)))
      .zip(coords.tail.map(Some(_)) :+ None)
      .map {
        case ((current, prev), next) => fn(prev, current, next)
      }
  }

}
