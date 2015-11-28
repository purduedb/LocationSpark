package cs.purdue.edu.spatialindex

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.quatree.QTree
import cs.purdue.edu.spatialindex.rtree._
import org.apache.commons.math3.distribution.MultivariateNormalDistribution

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Random._

/**
 * Created by merlin on 11/24/15.
 */
object testQuadtreeS {

  def GuassianRandom(MinX:Int): Int =
  {
    math.abs(nextGaussian()*100000%MinX).toInt
  }

  def gaussianPoint(mean:Array[Double]): Point =
  {
    val cov=Array.ofDim[Double](2,2)
    cov(0)(0)=110
    cov(0)(1)=1.5
    cov(1)(0)=1.5
    cov(1)(1)=60

    val generator=new MultivariateNormalDistribution(mean,cov)
    val data=generator.sample()

    Point(data(0).toFloat, data(1).toFloat)

  }

  def queryTimeofQtree[V](rt:QTree[V],boxes:Iterator[Box] ):Double={

    var b2=System.currentTimeMillis

    var emptyslot=0

    val emptyresultBoxs=new ArrayBuffer[Box]()

    boxes.foreach{
      box=> //println(box.toString)
        if(rt.search(box).size==0)
        {
          emptyslot=emptyslot+1
          emptyresultBoxs.+=(box)
        }
    }
    val rtreeQuerytime=(System.currentTimeMillis-b2)
    println("Qtree based range query time: "+rtreeQuerytime +" ms")
    println("# of empty slot: "+emptyslot)

    rtreeQuerytime
  }

  def uniformPoint(startx:Int, starty:Int, rangx:Int, rangy:Int):Point=
    Point(nextInt(rangx)+startx, nextInt(rangy)+starty)

  def build(es: Seq[Entry[Int]]): RTree[Int] =
    RTree(es: _*)

  def queryTimeofRtree(rt:RTree[Int],boxes:Iterator[Box] ):Double={

    var b2=System.currentTimeMillis

    var emptyslot=0

    val emptyresultBoxs=new ArrayBuffer[Box]()

    boxes.foreach{
      box=> //println(box.toString)
        if(rt.search(box).size==0)
        {
          emptyslot=emptyslot+1
          emptyresultBoxs.+=(box)
        }
      //println(rt.search(box).size)
    }
    val rtreeQuerytime=(System.currentTimeMillis-b2)
    println("Rtree based range query time: "+rtreeQuerytime +" ms")
    println("# of empty slot: "+emptyslot)


    b2=System.currentTimeMillis
    emptyslot=0
    emptyresultBoxs.foreach{
      box=> //println(box.toString)
        if(rt.search(box).size==0)
        {
          emptyslot=emptyslot+1
        }
      //println(rt.search(box).size)
    }
    println("Rtree query empty box time: "+(System.currentTimeMillis-b2) +" ms")
    println("# of empty slot: "+emptyslot)

    rtreeQuerytime
  }

  def main(args: Array[String]):Unit=
  {
    val numofpoints=400000
    val numofqueries=50000

    val mean1=Array(6.0,6.0)
    val mean2=Array(10.0,10.0)
    val mean3=Array(60.0,15.0)

    val es = (1 to numofpoints).map(n => Entry(gaussianPoint(mean1), n))
    val es2 = (1 to numofpoints).map(n => Entry(gaussianPoint(mean2), n))
    val es3 = (1 to numofpoints).map(n => Entry(gaussianPoint(mean3), n))

    val boxes = (1 to numofqueries).map{
      n =>
        val p1=uniformPoint(-(qtreeUtil.rangx/2),-qtreeUtil.rangy/2, (qtreeUtil.rangx/2),qtreeUtil.rangy/2)
        val p2=uniformPoint(1,1, 3,3)
        Box(p1.x,p1.y, p1.x+p2.x,p1.y+p2.y)
    }

    var b2=System.currentTimeMillis

     val qtree=QTree(es.toIterator)
    println("qtree size "+qtree.size)

    def dfunction(v1:Int,v2:Int):Int=
    {
      v1+v2
    }
    qtree.insertAll(es2,dfunction)
    println("qtree size "+qtree.size)

    qtree.insertAll(es3,dfunction)
    println("qtree size "+qtree.size)
    println("Qtree build time: "+(System.currentTimeMillis-b2) +" ms")
    queryTimeofQtree(qtree,boxes.toIterator)
    println("*"*100)

    var b1=System.currentTimeMillis
    var rt = build(es)
    rt=rt.insertAll(es2)
    rt=rt.insertAll(es3)
    println("Rtree build index time: "+(System.currentTimeMillis-b1) +" ms")
    queryTimeofRtree(rt,boxes.toIterator)

    println("*"*100)
    var hashset=new HashMap[Geom,Int]()

    b1=System.currentTimeMillis
    es.foreach(e=>hashset=hashset.+(e.geom->e.value))
    es2.foreach(e=>hashset=hashset.+(e.geom->e.value))
    es3.foreach(e=>hashset=hashset.+(e.geom->e.value))
    println("hashmap build index time: "+(System.currentTimeMillis-b1) +" ms")
/*
    b1=System.currentTimeMillis
    val ret=new ArrayBuffer[Geom]
    boxes.foreach{
      box=>hashset.foreach {
        case (geom: Geom, id) =>
          if(box.contains(geom))
          {
            ret.append(geom)
          }
      }
    }
    println("hashmap based range query time: "+(System.currentTimeMillis-b1) +" ms")*/

   /* es.foreach((e)=>qtree.contains(e.geom))

    es2.foreach((e)=>qtree.contains(e.geom))

    queryTimeofQtree(qtree,boxes)

    val insertpoint=Point(30, 38)
    val insertpoint2=Point(31, 38.3f)
    qtree.insert(insertpoint,100)
    qtree.insert(insertpoint2,101)

    assert(qtree.contains(insertpoint))

    val box=Box(-20,0,32,39)
    assert(qtree.search(box).contains(Entry(insertpoint,100)))

    qtree.remove(Entry(insertpoint,100))
    assert(!qtree.contains(insertpoint))

    println("*"*50)
    println(qtree.nearest(insertpoint))

    println("*"*50)
    qtree.insert(insertpoint,100)
    qtree.nearestK(insertpoint,2).foreach(println)

    println("*"*50)
    println(qtree.search(box).size)*/

    //println("*"*50)
    //println(qtree.entries.size)
    //qtree.entries.foreach(println)

   // qtree.printStructure()

  }
}
