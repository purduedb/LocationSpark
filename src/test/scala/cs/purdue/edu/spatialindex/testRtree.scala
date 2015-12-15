package cs.purdue.edu.spatialindex

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree.{RTree, Entry, Box}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 11/23/15.
 */
object testRtree {

  def main(args: Array[String]): Unit = {

    val numofqueries=10000
    val numofpoints=1000000

    val boxes = (1 to numofqueries).map{
      n =>
        val p1=qtreeUtil.getRandomUniformPoint(-(qtreeUtil.rangx/2),-qtreeUtil.rangy/2, (qtreeUtil.rangx/2),qtreeUtil.rangy/2)
        val p2=qtreeUtil.getRandomUniformPoint(1,1, 3,3)
        Box(p1.x,p1.y, p1.x+p2.x,p1.y+p2.y)
    }

    val boxes2=Array(
      Box(-17.10094f,-86.8612f, 18.41f, 80.222f),
      Box(-13.10094f,-87.8612f, 14.41f, 83.222f)
      ).map{
      box=>Entry(box,1)
    }

    //val boxtree=RTree(boxes: _*)


    val mean1=Array(6.0,6.0)
    val es = (1 to numofpoints).map(n => Entry(qtreeUtil.getGaussianPoint(mean1), n))
    val datatree=RTree(es: _*)

    //datatree.search(Box(-180,-180,180,180)).foreach(println)

    //boxtree.search(Box(-180,-180,180,180)).foreach(println)

    val insertbox=boxes.map{
      box=>Entry(box,1)
    }

    println("*"*100)
    var b1=System.currentTimeMillis

    val boxtree=RTree(insertbox: _*)
    val count1=datatree.join(boxtree).size

    println(count1)

    println("dual tree based for the sjoin time: "+(System.currentTimeMillis-b1) +" ms")

    println("*"*100)
    b1=System.currentTimeMillis

    val buf = ArrayBuffer.empty[Entry[Int]]

    boxes.foreach(box=>buf.appendAll(datatree.search(box)))

    val count2=buf.size

    println(count2)
    println("single tree based for the sjoin time: "+(System.currentTimeMillis-b1) +" ms")
    //boxtree.search(Box(-180,-180,180,180)).foreach(println)

    //boxtree.searchIntersection(Box(-50,-50,50,50)).foreach(println)
  }

}
