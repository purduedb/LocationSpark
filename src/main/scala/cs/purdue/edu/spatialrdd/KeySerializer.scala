package cs.purdue.edu.spatialrdd

import cs.purdue.edu.spatialindex.rtree._
import cs.purdue.edu.spatialindex.rtree.Geom

/**
 * Created by merlin on 8/7/15.
 */

/**
 * seralize the k v pair to rtree entry and point
 * @tparam K
 * @tparam V
 */
trait KeySerializer[K, V] extends Serializable {

  def toEntry(k: K, v:V): Entry[V]

}

class PointSerializer[K,V] extends Serializable{

  //as point
   def toEntry(k: K, value:V):  Entry[V]={
        Entry(k.asInstanceOf[Point],value)
  }

  def toEntry(k: K):  Entry[V]={
    k.asInstanceOf[Entry[V]]
  }

  def toPoint(k: K):  Point={
        k.asInstanceOf[Point]
  }

}


/*
class BoxSerializer[K,V] extends Serializable {

  override def toBox(k: K): Geom={

  }

}
*/




