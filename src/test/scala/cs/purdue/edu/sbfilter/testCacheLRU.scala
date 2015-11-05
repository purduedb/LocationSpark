package cs.purdue.edu.sbfilter

import cs.purdue.edu.spatialbloomfilter.SBFilter
import cs.purdue.edu.spatialindex.quatree.{QTreeCache, QTree}
import cs.purdue.edu.spatialindex.rtree.{Box, RTree, Entry, Point}

import scala.util.Random._

/**
 * Created by merlin on 11/5/15.
 */
object testCacheLRU {

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
    }
    val rtreeQuerytime=(System.currentTimeMillis-b2)
    println("Rtree based range query time: "+rtreeQuerytime +" ms")
    println("empty slot: "+emptyslot)
    println("* "*50)

    //val box1 = Box(1,1,100,140)
    //val results1 = rt.search(box1).toSet

    b2=System.currentTimeMillis
    val qtree=new QTree(1000)
    boxes.foreach{
      box=> //println(box.toString)
        if(rt.search(box).size==0)
        {
          qtree.insertBox(box)
        }
    }
    println("merge all true branch "+qtree.mergeBranchWIthAllTrueLeafNode())
    val sbfilter=SBFilter(qtree.getSBFilter())
    println("build SBfilter without cache time: "+(System.currentTimeMillis-b2-rtreeQuerytime) +" ms")
    println("* "*50)

    Thread.sleep(10000)

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
    println("* "*50)

    Thread.sleep(10000)

    b2=System.currentTimeMillis
    boxes.foreach{
      box=> //println(box.toString)
        if(sbfilter.searchRectangle(box))
        {
          rt.search(box)
        }
      //println(rt.search(box).size)
    }

    println("rtree with binnary sbfilter time: "+(System.currentTimeMillis-b2) +" ms")
    println("* "*50)
   /*

    b2=System.currentTimeMillis
    val qtree2=new QTreeCache(1000)
    boxes.foreach{
      box=> //println(box.toString)
        if(rt.search(box).size==0)
        {
          qtree2.insertBoxWithCache(box)
        }
    }

    val sbfilter2=SBFilter(qtree2.getSBFilter())
    println("build sbfilter with cach mechanism time: "+(System.currentTimeMillis-b2-rtreeQuerytime) +" ms")
    println("* "*50)

   b2=System.currentTimeMillis
    boxes.foreach{
      box=> //println(box.toString)
        if(qtree2.queryBoxWithCache(box))
        {
          rt.search(box)
        }
      //println(rt.search(box).size)
    }
    println("rtree with qtree2cache time: "+(System.currentTimeMillis-b2) +" ms")
    println("* "*50)
    */


    //qtree.printTreeStructure()

    /*
    b2=System.currentTimeMillis
    qtree2.mergeNodes(10000)
    println("merge index time: "+(System.currentTimeMillis-b2) +" ms")
    println("* "*50)
    */


  }

}
