package  cs.purdue.edu.spatialindex.rtree

/**
 * This is a small ADT that we use to avoid building too many
 * intermediate vectors.
 *
 * It allows us to concatenate a whole bunch of vectors or single
 * elements cheaply, and then iterate over them later.
 */
sealed trait Joined[V] extends Iterable[V] {

  def iterator: Iterator[V]

  override def isEmpty: Boolean = false

  def ++(that: Joined[V]): Joined[V] =
    if (that.isEmpty) this else Joined.Concat(this, that)

  override def hashCode(): Int =
    iterator.foldLeft(0x0a704453)((x, y) => x + (y.## * 0xbb012349 + 0x337711af))

  override def equals(that: Any): Boolean =
    that match {
      case that: Joined[_] =>
        val it1 = this.iterator
        val it2 = that.iterator
        while (it1.hasNext && it2.hasNext) {
          if (it1.next != it2.next) return false //scalastyle:off
        }
        it1.hasNext == it2.hasNext
      case _ =>
        false
    }
}

object Joined {
  def empty[V]: Joined[V] = Wrapped(Vector.empty)
  def apply[V](a: V): Joined[V] = Single(a)
  def wrap[V](as: Vector[V]): Joined[V] = Wrapped(as)

  case class Single[V](a: V) extends Joined[V] {
    def iterator: Iterator[V] = Iterator(a)
  }

  case class Wrapped[V](as: Vector[V]) extends Joined[V] {
    override def isEmpty: Boolean = as.isEmpty
    def iterator: Iterator[V] = as.iterator
    override def ++(that: Joined[V]): Joined[V] =
      if (this.isEmpty) that else if (that.isEmpty) this else Joined.Concat(this, that)
  }

  case class Concat[V](x: Joined[V], y: Joined[V]) extends Joined[V] {
    def iterator: Iterator[V] = x.iterator ++ y.iterator
  }
}

