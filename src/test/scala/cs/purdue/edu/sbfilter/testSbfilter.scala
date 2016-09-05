package cs.purdue.edu.sbfilter

import cs.purdue.edu.spatialindex.quatree.{QTree, SBQTree}
import cs.purdue.edu.spatialindex.rtree._
import cs.purdue.edu.spatialindex.spatialbloomfilter.{qtreeUtil, SBFilter}
import org.apache.commons.math3.distribution.MultivariateNormalDistribution
import scala.collection.mutable.ArrayBuffer
import scala.util.Random._

/**
 * Created by merlin on 10/27/15.
 */
object testSbfilter {


  def GuassianRandom(MinX:Int): Int =
  {
    math.abs(nextGaussian()*100000%MinX).toInt
  }

  def gaussianPoint(mean:Array[Double]): Point =
  {
    val cov=Array.ofDim[Double](2,2)

    //val cov2=new Array[Array[Double]](2,2)

    /*cov(0)(0)=500
    cov(0)(1)=0.4
    cov(1)(0)=0.4
    cov(1)(1)=500*/

    cov(0)(0)=110
    cov(0)(1)=1.5
    cov(1)(0)=1.5
    cov(1)(1)=60

    val generator=new MultivariateNormalDistribution(mean,cov)
    val data=generator.sample()

    Point(data(0).toFloat, data(1).toFloat)

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
          emptyresultBoxs.+=(box)
        }
      //println(rt.search(box).size)
    }
    val rtreeQuerytime=(System.currentTimeMillis-b2)
    println("Rtree based range query time: "+rtreeQuerytime +" ms")
    println("# of empty slot: "+emptyresultBoxs.size)


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


  /*def learnFromKNN(rt:RTree[Int],datapoint:Iterator[Geom]) :SBQTree={


  }*/


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

    println("build SBQtree  time: "+(rtreeQuerytime-(System.currentTimeMillis-b2)) +" ms")

    qtree
  }

  def queryTimeOfSBQtree(rt:RTree[Int],boxes:Iterator[Box], qtree:SBQTree)=
  {
    //qtree.printTreeStructure()
   // println("merge all true branch "+qtree.mergeBranchWIthAllTrueLeafNode())

    var count=0
    var count2=0
    var falseNegative=0

    var timeforQtree=0.0

    boxes.foreach{
      box=> //println(box.toString)
        var b2=System.currentTimeMillis
        var sResult=qtree.queryBox(box)
        timeforQtree+=System.currentTimeMillis-b2

        var rResult=rt.search(box).size>0

        if(sResult==rResult)
        //if(qtree.queryBoxWithP(box)<0.2)
        {
          count+=1
        }else
        {
          count2+=1
        }

        if(sResult==false&&rResult==true)
        {
          falseNegative+=1
        }
      //println(rt.search(box).size)
    }

    println("rtree with naive-sbqtree time: "+(timeforQtree) +" ms")
    println("correct: "+count)
    println("false: "+count2)
    println("false negative: "+falseNegative)

  }

  def queryTimeOfSBfilter(rt:RTree[Int],boxes:Iterator[Box], qtree:SBQTree): Unit =
  {

    println("merge all true branch "+qtree.mergeBranchWIthAllTrueLeafNode())
    //qtree.printTreeStructure()

    val sbfilter=SBFilter(qtree.getSBFilter())

    var timeforQtree=0.0

    var count=0
    boxes.foreach{
      box=> //println(box.toString)
        //if(sbfilter.searchRectangleWithP(box)<0.2)
        val b2=System.currentTimeMillis
        if(sbfilter.searchRectangle(box)==false)
        {
          //rt.search(box)
          count+=1
        }

        timeforQtree+=System.currentTimeMillis-b2
    }
    println("rtree with binnary sbfilter time: "+(timeforQtree) +" ms")
    println("empty slot: "+count)
  }

  def queryTimeOfWithTrainSBfilter(rt:RTree[Int],boxes:Iterator[Box], sbfilter:SBFilter): Unit =
  {

    var timeforQtree=0.0

    var count=0
    boxes.foreach{
      box=> //println(box.toString)
        //if(sbfilter.searchRectangleWithP(box)<0.2)
        val b2=System.currentTimeMillis
        if(sbfilter.searchRectangle(box)==false)
        {
          //rt.search(box)
          count+=1
        }

        timeforQtree+=System.currentTimeMillis-b2
    }
    println("rtree with trained binnary sbfilter time: "+(timeforQtree) +" ms")
    println("empty slot: "+count)
  }

  def queryTimeOfSBfilterV2(rt:RTree[Int],boxes:Iterator[Box], qtree:SBQTree): Unit =
  {

    println("merge all true branch "+qtree.mergeBranchWIthAllTrueLeafNode())
    //qtree.printTreeStructure()

    val sbfilter=SBFilter(qtree.getSBFilterV2())
    var timeforQtree=0.0

    var count=0
    boxes.foreach{
      box=> //println(box.toString)
        var b2=System.currentTimeMillis
        if(sbfilter.searchRectangleV2(box)==false)
        {
         // rt.search(box)
          count+=1
        }

        timeforQtree+=System.currentTimeMillis-b2
    }
    println("binnary sbfilterv2 query time: "+(timeforQtree) +" ms")
    println("empty slot: "+count)
    println("*"*50)

   // b2=System.currentTimeMillis

    //println("r-tree with binnary sbfilterv2 actually run time: "+(System.currentTimeMillis-b2) +" ms")

  }

  def trainTimeSBFilter(datapoint:Iterator[Geom]):SBFilter=
  {
    val b2=System.currentTimeMillis
    val qtree=new SBQTree(1000)
    qtree.trainSBfilter(datapoint)

    //qtree.printTreeStructure()
    println("train the sbfilter time: "+(System.currentTimeMillis-b2) +" ms")
    SBFilter(qtree.getSBFilterV2())

  }

  def updateSBFilter(datapoint:Iterator[Geom],boxes:Iterator[Box],rt:RTree[Int]):SBQTree=
  {
    var b2=System.currentTimeMillis
    val qtree=new SBQTree(1000)
    qtree.trainSBfilter(datapoint)
    //qtree.printTreeStructure()
    println("train the sbfilter time: "+(System.currentTimeMillis-b2) +" ms")

    b2=System.currentTimeMillis

    boxes.foreach{
      box=> //println(box.toString)
        if(rt.search(box).size==0)
        {
          qtree.insertBox(box)
        }
      //println(rt.search(box).size)
    }
    println("update the time: "+(System.currentTimeMillis-b2) +" ms")

    qtree

  }


  def queryTimeofTrainSBfilter(rt:RTree[Int],boxes:Iterator[Box], sbfilter:SBFilter)={

    val b2=System.currentTimeMillis
    var count=0
    boxes.foreach{
      box=> //println(box.toString)
       //if(sbfilter.searchRectangleWithP(box)<0.2)
        if(!sbfilter.searchRectangleV2(box))
        {
         // rt.search(box)
          count+=1
        }
    }
    println("query by trained binnary sbfilter time: "+(System.currentTimeMillis-b2) +" ms")
    println("empty slot: "+count)

  }

  def main(args: Array[String]): Unit = {

    val numofpoints=200000
    val numofqueries=50000

    val mean1=Array(6.0,6.0)
    val mean2=Array(10.0,10.0)
    val mean3=Array(60.0,15.0)

    val es = (1 to numofpoints).map(n => Entry(gaussianPoint(mean1), n))
    val es2 = (1 to numofpoints).map(n => Entry(gaussianPoint(mean2), n))
    val es3 = (1 to numofpoints).map(n => Entry(gaussianPoint(mean3), n))
    //val es4 = (1 to numofpoints).map(n => Entry(gaussianPoint(600,1300,3000,5000), n))
    //val es5 = (1 to numofpoints).map(n => Entry(gaussianPoint(1500,2300,3000,3000), n))
    //val es6 = (1 to numofpoints).map(n => Entry(gaussianPoint(200,300,800,800), n))

    val boxes = (1 to numofqueries).map{
      n =>
      val p1=uniformPoint(-(qtreeUtil.rangx/2),-qtreeUtil.rangy/2, (qtreeUtil.rangx/2),qtreeUtil.rangy/2)
      val p2=uniformPoint(1,1, 3,3)
      Box(p1.x,p1.y, p1.x+p2.x,p1.y+p2.y)
    }

    var b1=System.currentTimeMillis
    var rt = build(es)
    rt=rt.insertAll(es2)
    rt=rt.insertAll(es3)

    //rt=rt.insertAll(es6)
    println("build rtree index time: "+(System.currentTimeMillis-b1) +" ms")

    b1=System.currentTimeMillis
    val Quadtree=QTree(es: _*)
    Quadtree.insertAll(es2)
    Quadtree.insertAll(es3)
    println("build quadtree index time: "+(System.currentTimeMillis-b1) +" ms")

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

    //get the query time by SBFilterv2 with the precomputed value
    queryTimeOfSBfilterV2(rt,boxes.toIterator,sbqtree)
    println("*"*100)

    /*


    //get the query time by SBQtree speedup
    queryTimeOfSBQtree(rt,boxes.toIterator,sbqtree)
    println("*"*100)

    //get the query time by SBFilter Speedup
    queryTimeOfSBfilter(rt,boxes.toIterator,sbqtree)
    println("*"*100)

    val inputdata=(es.++:(es2).++:(es3)).map{entry=>entry.geom}.toIterator
    val sbfilter=trainTimeSBFilter(inputdata)
    println("*"*100)

    queryTimeOfWithTrainSBfilter(rt,boxes.toIterator,sbfilter)
    println("*"*100)

    queryTimeOfSBfilterV2(rt,boxes.toIterator,sbqtree)
    println("*"*100)

    //train the sbfilter time
    val inputdata=(es.++:(es2).++:(es3)).map{entry=>entry.geom}.toIterator
    val sbfilter=trainTimeSBFilter(inputdata)
    println("*"*100)

    //query over the trained sbfilter time
    queryTimeofTrainSBfilter(rt,boxes.toIterator,sbfilter)
    println("*"*100)

    val updatesbfliter=updateSBFilter(inputdata,boxes.toIterator,rt)

    queryTimeOfSBfilterV2(rt,boxes.toIterator,updatesbfliter)

   // println("*"*100)
   */

  }
}
