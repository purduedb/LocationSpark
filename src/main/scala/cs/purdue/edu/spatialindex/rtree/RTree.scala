package cs.purdue.edu.spatialindex.rtree

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.math.{min, max}
import scala.util.Try

object RTree {

  /**
   * Construct an empty RTree.
   */
  def empty[V]: RTree[V] = new RTree(Node.empty[V], 0)

  /**
   * Construct an RTree from a sequence of entries.
   */
  def apply[V](entries: Entry[V]*): RTree[V] =
    entries.foldLeft(RTree.empty[V])(_ insert _)

  def apply[A](itr:Iterator[Entry[A]])=
  {
    itr.foldLeft(RTree.empty[A])(_ insert _)
  }
}

/**
 * This is the magnificent RTree, which makes searching ad-hoc
 * geographic data fast and fun.
 *
 * The RTree wraps a node called 'root' that is the actual root of the
 * tree structure. RTree also keeps track of the total size of the
 * tree (something that individual nodes don't do).
 */
case class RTree[V](root: Node[V], size: Int) {

  /**
   * Typesafe equality test.
   *
   * In order to be considered equal, two trees must have the same
   * number of entries, and each entry found in one should be found in
   * the other.
   */
  def ===(that: RTree[V]): Boolean =
    size == that.size && entries.forall(that.contains)

  /**
   * Universal equality test.
   *
   * Trees can only be equal to other trees. Unlike some other
   * containers, the trees must be parameterized on the same type, or
   * else the comparison will fail.
   *
   * This means comparing an RTree[Int] and an RTree[BigInt] will
   * always return false.
   */
  override def equals(that: Any): Boolean =
    that match {
      case rt: RTree[_] =>
        Try(this === rt.asInstanceOf[RTree[V]]).getOrElse(false)
      case _ =>
        false
    }

  /**
   * Universal hash code method.
   */
  override def hashCode(): Int = {
    var x = 0xbadd0995
    val it = entries
    while (it.hasNext) x ^= (it.next.hashCode * 777 + 1)
    x
  }

  /**
   * Insert a value into the tree at (x, y), returning a new tree.
   */
  def insert(x: Float, y: Float, value: V): RTree[V] =
    insert(Entry(Point(x, y), value))


  /**
   * Insert an entry into the tree, returning a new tree.
   */
  def insert(entry: Entry[V]): RTree[V] = {
    val r = root.insert(entry) match {
      case Left(rs) => Branch(rs, rs.foldLeft(Box.empty)(_ expand _.box))
      case Right(r) => r
    }
    RTree(r, size + 1)
  }

  /**
   * Insert a value into the tree at (x, y), returning a new tree.
   */
  def insert(x:Point, value: V): RTree[V] =
    insert(Entry(x, value))


  /**
   * Insert entries into the tree, returning a new tree.
   */
  def insertAll(entries: Iterable[Entry[V]]): RTree[V] =
    entries.foldLeft(this)(_ insert _)

  /**
   * Remove an entry from the tree, returning a new tree.
   *
   * If the entry was not present, the tree is simply returned.
   */
  def remove(entry: Entry[V]): RTree[V] =
    root.remove(entry) match {
      case None =>
        this
      case Some((es, None)) =>
        es.foldLeft(RTree.empty[V])(_ insert _)
      case Some((es, Some(node))) =>
        es.foldLeft(RTree(node, size - es.size - 1))(_ insert _)
    }

  /**
   * Remove entries from the tree, returning a new tree.
   */
  def removeAll(entries: Iterable[Entry[V]]): RTree[V] =
    entries.foldLeft(this)(_ remove _)

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def search(space: Box): Seq[Entry[V]] =
    root.search(space, _ => true)

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def searchPoint(pt: Point): Entry[V] =
  {
    val ret=root.search(pt.toBox, _ => true)

    if(ret!=null&&ret.length!=0)
      ret.head
    else
      null

  }


  /**
   * Return a sequence of all entries found in the given search space.
   */
  def search(space: Box, f: Entry[V] => Boolean): Seq[Entry[V]] =
    root.search(space, f)



  /**
   * Return a sequence of all entries intersecting the given search space.
   */
  def searchIntersection(space: Box): Seq[Entry[V]] =
    root.searchIntersection(space, _ => true)

  /**
   * Return a sequence of all entries intersecting the given search space.
   */
  def searchIntersection(space: Box, f: Entry[V] => Boolean): Seq[Entry[V]] =
    root.searchIntersection(space, f)

  /**
   * Construct a result an initial value, the entries found in a
   * search space, and a binary function `f`.
   *
   *   rtree.foldSearch(space, init)(f)
   *
   * is equivalent to (but more efficient than):
   *
   *   rtree.search(space).foldLeft(init)(f)
   */
  def foldSearch[B](space: Box, init: B)(f: (B, Entry[V]) => B): B =
    root.foldSearch(space, init)(f)

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearest(pt: Point): Option[Entry[V]] =
    root.nearest(pt, Float.PositiveInfinity).map(_._2)

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearestK(pt: Point, k: Int): IndexedSeq[Entry[V]] =
    if (k < 1) {
      Vector.empty
    } else {
      implicit val ord = Ordering.by[(Double, Entry[V]), Double](_._1)
      val pq = PriorityQueue.empty[(Double, Entry[V])]
      root.nearestK(pt, k, Double.PositiveInfinity, pq)
      val arr = new Array[Entry[V]](pq.size)
      var i = arr.length - 1
      while (i >= 0) {
        val (_, e) = pq.dequeue
        arr(i) = e
        i -= 1
      }
      arr
    }

  /**
   * Return a count of all entries found in the given search space.
   */
  def count(space: Box): Int =
    root.count(space)

  /**
   * Return whether or not the value exists in the tree at (x, y).
   */
  def contains(x: Float, y: Float, value: V): Boolean =
    root.contains(Entry(Point(x, y), value))

  /**
   * Return whether or not the given entry exists in the tree.
   */
  def contains(entry: Entry[V]): Boolean =
    root.contains(entry)

  /**
   * Map the entry values from A to B.
   */
  def map[B](f: V => B): RTree[B] =
    RTree(root.map(f), size)

  /**
   * Return an iterator over all entries in the tree.
   */
  def entries: Iterator[Entry[V]] =
    root.iterator

  /**
   * Return an iterator over all values in the tree.
   */
  def values: Iterator[V] =
    entries.map(_.value)

  /**
   * Return a nice depiction of the tree.
   *
   * This method should only be called on small-ish trees! It will
   * print one line for every branch, leaf, and entry, so for a tree
   * with thousands of entries this will result in a very large
   * string!
   */
  def pretty: String = root.pretty

  override def toString: String =
    s"RTree(<$size entries>)"
}
