package cs.purdue.edu.spatialindex

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree._

import scala.collection.mutable
import scala.collection.mutable.{PriorityQueue, ArrayBuffer}

/**
 * Created by merlin on 11/23/15.
 */
object testRtree {

  def main(args: Array[String]): Unit = {

    val numofqueries=900
    val numofpoints=5000

    /**/

    val mean1=Array(6.0,6.0)
    val es = (1 to numofpoints).map(n => Entry(qtreeUtil.getGaussianPoint(mean1), n))
    val e2 = (1 to numofpoints).map(n => Entry(qtreeUtil.getGaussianPoint(mean1), n))

    val datatree=RTree(es: _*)

    val datatree2=RTree(e2: _*)

    val querypoint=es.take(10).map(e=>e.geom)

    val k=20

    val ret=ArrayBuffer.empty[(Geom, Double, Iterator[(Geom,Int)])]

    def f1(k:Geom):Boolean=
    {
      true
    }

    def f2(v:Any):Boolean=
    {
      v.toString.toInt==10
    }

    def filtercondition[V](entry:Entry[V]):Boolean=
    {
      f1(entry.geom)&&f2(entry.value)
    }

    //nest loop knn search
    querypoint.foreach{
      case(p:Point)=>

        val knnqueryresult=datatree.nearestK(p,k,filtercondition)
        val tmp=knnqueryresult.map{
          case(distance,entry)=>
            (entry.geom,entry.value)
        }.toIterator

        ret.append((p, knnqueryresult.last._1, tmp))
      case _=>
    }

    val hashMap=mutable.HashMap.empty[Geom,(Geom,Double,mutable.PriorityQueue[(Double, (Geom,Int))])]

    val boxes=ret.map{

      case(qpoint,maxdistance,itr)=>

        val p=qpoint.asInstanceOf[Point]

        implicit val ord = Ordering.by[(Double, (Geom,Int)), Double](_._1)
        val pq = PriorityQueue.empty[(Double, (Geom,Int))]

        itr.foreach
        {
          case (location,value)=>
            val localdistance=p.distance(location.asInstanceOf[Point])
            pq += ((localdistance, (location,value)))
        }

        val qbox=Box((p.x-maxdistance).toFloat,(p.y-maxdistance).toFloat,(p.x+maxdistance).toFloat,(p.y+maxdistance).toFloat)

        hashMap.put(qbox,(qpoint,maxdistance,pq))

        (qbox)
    }

    val insertbox=boxes.map
    {
      case(b)=>Entry(b,1)
    }

    val boxtree= RTree(insertbox: _*)

    val knnjoin=datatree2.rjoinforknn(boxtree,hashMap, f1,f2)

    knnjoin.foreach{
      case(pt, itr)=>
        print(pt+" ")
        itr.foreach(print)
        println
    }

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

/**
 *
 * //for testing the range join
val boxes = (1 to numofqueries).map{
      n =>
        val p1=qtreeUtil.getRandomUniformPoint(-(qtreeUtil.rangx/2),-qtreeUtil.rangy/2, (qtreeUtil.rangx/2),qtreeUtil.rangy/2)
        val p2=qtreeUtil.getRandomUniformPoint(1,1, 6,6)
        Box(p1.x,p1.y, p1.x+p2.x,p1.y+p2.y)
    }

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
  */