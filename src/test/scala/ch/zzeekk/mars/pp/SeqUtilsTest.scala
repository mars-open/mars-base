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

import ch.zzeekk.mars.pp.utils.SeqUtils.withPrevAndNext
import org.scalatest.funsuite.AnyFunSuite

class SeqUtilsTest extends AnyFunSuite {

  test("process withPrevAndNext Seq of size 1") {
    val xs = Seq("first")
    val result = withPrevAndNext(xs) {
      case (prev,current,next) =>
        assert(prev.isEmpty)
        assert(next.isEmpty)
        current
    }
    val expected = xs
    assert(result == expected)
  }

  test("process withPrevAndNext Seq of size 2") {
    val xs = Seq("first","second")
    val result = withPrevAndNext(xs) {
      case (prev,current,next) =>
        if (current=="first") assert(prev.isEmpty)
        else assert(prev.isDefined)
        if (current=="second") assert(next.isEmpty)
        else assert(next.isDefined)
        current
    }
    val expected = xs
    assert(result == expected)
  }

  test("process withPrevAndNext Seq of size 3") {
    val xs = Seq("first","second","third")
    val result = withPrevAndNext(xs) {
      case (prev,current,next) =>
        if (current=="first") assert(prev.isEmpty)
        else assert(prev.isDefined)
        if (current=="third") assert(next.isEmpty)
        else assert(next.isDefined)
        current
    }
    val expected = xs
    assert(result == expected)
  }

}
