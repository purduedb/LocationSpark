package cs.purdue.edu.spatialindex

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree.{RTree, Entry, Box}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 11/23/15.
 */
object testRtree {

  def main(args: Array[String]): Unit = {

    val numofqueries=900000
    val numofpoints=1000000

    val boxes = (1 to numofqueries).map{
      n =>
        val p1=qtreeUtil.getRandomUniformPoint(-(qtreeUtil.rangx/2),-qtreeUtil.rangy/2, (qtreeUtil.rangx/2),qtreeUtil.rangy/2)
        val p2=qtreeUtil.getRandomUniformPoint(1,1, 6,6)
        Box(p1.x,p1.y, p1.x+p2.x,p1.y+p2.y)
    }

    val mean1=Array(6.0,6.0)
    val es = (1 to numofpoints).map(n => Entry(qtreeUtil.getGaussianPoint(mean1), n))
    val datatree=RTree(es: _*)

    val insertbox=boxes.map{
      box=>Entry(box,1)
    }

    def aggfunction1[K,V](itr:Iterator[(K,V)]):Int=
    {
      itr.size
    }

    def aggfunction2(v1:Int, v2:Int):Int=
    {
      v1+v2
    }

    val boxtree=RTree(insertbox: _*)

    println("*"*100)
    var b1=System.currentTimeMillis

    val tmp=datatree.joins_withoutsort(boxtree)(aggfunction1,aggfunction2)

    println(tmp.size)

    println("dual tree based for the sjoin time: "+(System.currentTimeMillis-b1) +" ms")

    datatree.sortInternalnode()
    boxtree.sortInternalnode()

    println("*"*100)
    b1=System.currentTimeMillis

    val tmp2=datatree.joins(boxtree)(aggfunction1,aggfunction2)
    println(tmp.size)
    println("dual tree with soring based for the sjoin time: "+(System.currentTimeMillis-b1) +" ms")

    //boxtree.search(Box(-180,-180,180,180)).foreach(println)

    //boxtree.searchIntersection(Box(-50,-50,50,50)).foreach(println)
  }

}

/**
 *    val boxes2=Array(
      Box(-17.10094f,-86.8612f, 18.41f, 80.222f),
      Box(-13.10094f,-87.8612f, 14.41f, 83.222f)
      ).map{
      box=>Entry(box,1)
    }

 */