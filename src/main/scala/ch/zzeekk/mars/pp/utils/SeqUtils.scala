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
