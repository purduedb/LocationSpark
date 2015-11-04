package cs.purdue.edu.sbfilter

import cs.purdue.edu.spatialbloomfilter.{SBFilter, qtreeUtil}
import cs.purdue.edu.spatialindex.quatree.QTree
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

    //val box1 = Box(1,1,100,140)
    //val results1 = rt.search(box1).toSet
    val qtree=new QTree(1000)

    b2=System.currentTimeMillis
    boxes.foreach{
      box=> //println(box.toString)
         if(rt.search(box).size==0)
         {
           qtree.insertBox(box)
         }
         //println(rt.search(box).size)
    }
    //println("number of output tuples: "+results1.size)
    val sbfilter=SBFilter(qtree.getSBFilter())
    println("build SBfilter time: "+(System.currentTimeMillis-b2-rtreeQuerytime) +" ms")

    //qtree.printTreeStructure()

    b2=System.currentTimeMillis

    boxes.foreach{
      box=> //println(box.toString)
        if(qtree.queryBox(box))
        {
          rt.search(box)
        }
      //println(rt.search(box).size)
    }

    println("rtree with naive-qtree time: "+(System.currentTimeMillis-b2) +" ms")

    b2=System.currentTimeMillis

    boxes.foreach{
      box=> //println(box.toString)
        if(sbfilter.searchRectangleWithP(box)>0.7)
        {
          rt.search(box)
        }
      //println(rt.search(box).size)
    }

    println("rtree with binnary sbfilter time: "+(System.currentTimeMillis-b2) +" ms")


    /*b2=System.currentTimeMillis

    boxes.foreach{
      box=> //println(box.toString)
        if(sbfilter.searchRectangleV2(box))
        {
          rt.search(box)
        }
      //println(rt.search(box).size)
    }

    println("rtree with speed sbfilter V2 time: "+(System.currentTimeMillis-b2) +" ms")
    */




  }
}
