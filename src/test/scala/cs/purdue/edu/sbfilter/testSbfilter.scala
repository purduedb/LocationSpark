package cs.purdue.edu.sbfilter

import cs.purdue.edu.spatialbloomfilter.{SBFilter, qtreeUtil}
import cs.purdue.edu.spatialindex.quatree.{SBQTree}
import cs.purdue.edu.spatialindex.rtree._
import scala.util.Random._

/**
 * Created by merlin on 10/27/15.
 */
object testSbfilter {


  def GuassianRandom(MinX:Int): Int =
  {
    math.abs(nextGaussian()*100000%MinX).toInt
  }

  def gaussianPoint(startx:Int, starty:Int,rangx:Int, rangy:Int): Point =
    Point(GuassianRandom(rangx)+startx, GuassianRandom(rangy)+starty)

  def uniformPoint(startx:Int, starty:Int, rangx:Int, rangy:Int):Point=
    Point(nextInt(rangx)+startx, nextInt(rangy)+starty)

  def build(es: Seq[Entry[Int]]): RTree[Int] =
    RTree(es: _*)


  def queryTimeofRtree(rt:RTree[Int],boxes:Iterator[Box] ):Double={

    var b2=System.currentTimeMillis

    var emptyslot=0

    boxes.foreach{
      box=> //println(box.toString)
        if(rt.search(box).size==0)
        {
          emptyslot=emptyslot+1
        }
      //println(rt.search(box).size)
    }
    val rtreeQuerytime=(System.currentTimeMillis-b2)
    println("Rtree based range query time: "+rtreeQuerytime +" ms")
    println("empty slot: "+emptyslot)
    rtreeQuerytime
  }

  def buildTimeofSBQtree( rt:RTree[Int], boxes:Iterator[Box], rtreeQuerytime:Double):SBQTree={

    //val box1 = Box(1,1,100,140)
    //val results1 = rt.search(box1).toSet
    val qtree=new SBQTree(1000)

    val b2=System.currentTimeMillis

    boxes.foreach{
      box=> //println(box.toString)
        if(rt.search(box).size==0)
        {
          qtree.insertBox(box)
        }
      //println(rt.search(box).size)
    }

    println("build SBQtree  time: "+(System.currentTimeMillis-b2-rtreeQuerytime) +" ms")

    qtree
  }

  def queryTimeOfSBQtree(rt:RTree[Int],boxes:Iterator[Box], qtree:SBQTree)=
  {
    //qtree.printTreeStructure()
   // println("merge all true branch "+qtree.mergeBranchWIthAllTrueLeafNode())

    var b2=System.currentTimeMillis
    var count=0
    boxes.foreach{
      box=> //println(box.toString)
        if(qtree.queryBox(box))
        {
          rt.search(box)
          count+=1
        }
      //println(rt.search(box).size)
    }

    println("rtree with naive-sbqtree time: "+(System.currentTimeMillis-b2) +" ms")
    println("empty slot: "+count)
  }

  def queryTimeOfSBfilter(rt:RTree[Int],boxes:Iterator[Box], qtree:SBQTree): Unit =
  {

    println("merge all true branch "+qtree.mergeBranchWIthAllTrueLeafNode())
    //qtree.printTreeStructure()

    val sbfilter=SBFilter(qtree.getSBFilter())

    val b2=System.currentTimeMillis
    var count=0
    boxes.foreach{
      box=> //println(box.toString)
        //if(sbfilter.searchRectangleWithP(box)>0.7)
        if(sbfilter.searchRectangle(box))
        {
          rt.search(box)
          count+=1
        }
    }
    println("rtree with binnary sbfilter time: "+(System.currentTimeMillis-b2) +" ms")
    println("empty slot: "+count)
  }

  def trainTimeSBFilter(datapoint:Iterator[Geom]):SBFilter=
  {
    val b2=System.currentTimeMillis
    val qtree=new SBQTree(1000)
    val sbdata=qtree.trainSBfilter(datapoint)

    qtree.printTreeStructure()

    println("train the sbfilter time: "+(System.currentTimeMillis-b2) +" ms")
    SBFilter(sbdata)
  }

  def queryTimeofTrainSBfilter(rt:RTree[Int],boxes:Iterator[Box], sbfilter:SBFilter)={

    val b2=System.currentTimeMillis
    var count=0
    boxes.foreach{
      box=> //println(box.toString)
       // if(sbfilter.searchRectangleWithP(box)>0.7)
        if(sbfilter.searchRectangle(box))
        {
          rt.search(box)
          count+=1
        }
    }
    println("rtree with trained binnary sbfilter time: "+(System.currentTimeMillis-b2) +" ms")
    println("empty slot: "+count)

  }

  def main(args: Array[String]): Unit = {

    val numofpoints=20000
    val numofqueries=10000

    val es = (1 to numofpoints).map(n => Entry(gaussianPoint(100,100,400,500), n))
    val es2 = (1 to numofpoints).map(n => Entry(gaussianPoint(600,600,2000,2000), n))
    val es3 = (1 to numofpoints).map(n => Entry(gaussianPoint(1500,1500,3000,5000), n))

    val boxes = (1 to numofqueries).map{
      n =>
      val p1=uniformPoint(100,10, 1000,3000)
      val p2=gaussianPoint(0,0,200,200)
      Box(p1.x,p1.y, p1.x+p2.x,p1.y+p2.y)
    }

    val b1=System.currentTimeMillis
    var rt = build(es)
    rt=rt.insertAll(es2)
    rt=rt.insertAll(es3)
    println("build rtree index time: "+(System.currentTimeMillis-b1) +" ms")

    //get the rtree baseline query time
    val rtreequerytime=queryTimeofRtree(rt,boxes.toIterator)
    println("*"*100)

    //get the SBQtree build time
    val sbqtree=buildTimeofSBQtree(rt,boxes.toIterator,rtreequerytime)
    println("*"*100)

    //get the query time by SBQtree speedup
    queryTimeOfSBQtree(rt,boxes.toIterator,sbqtree)
    println("*"*100)

    //get the query time by SBFilter Speedup
    queryTimeOfSBfilter(rt,boxes.toIterator,sbqtree)
    println("*"*100)

    /*
    //train the sbfilter time
    val inputdata=(es.++:(es2).++:(es3)).map{entry=>entry.geom}.toIterator
    val sbfilter=trainTimeSBFilter(inputdata)
    println("*"*100)

    //query over the trained sbfilter time
    queryTimeofTrainSBfilter(rt,boxes.toIterator,sbfilter)
    println("*"*100)
    */

  }
}
