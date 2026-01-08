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
