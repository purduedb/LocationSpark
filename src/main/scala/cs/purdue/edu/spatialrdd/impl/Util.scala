package cs.purdue.edu.spatialrdd.impl

import cs.purdue.edu.spatialindex.rtree.{Point, Entry}

import scala.util.Random._

/**
 * Created by merlin on 9/20/15.
 */
object Util{

  def toEntry[K,V](k: K, value:V) :Entry[V]={
    Entry(k.asInstanceOf[Point],value)
  }

  def toPoint[K](k: K):  Point={
    k.asInstanceOf[Point]
  }

  def uniformPoint(rangex:Int, rangey:Int):Point=
    Point(nextInt(rangex-2)+2, nextInt(rangey-2)+2)
}
