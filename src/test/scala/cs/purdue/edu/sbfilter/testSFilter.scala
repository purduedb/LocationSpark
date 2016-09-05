package cs.purdue.edu.sbfilter

import java.io.File

import com.vividsolutions.jts.io.WKTReader
import cs.purdue.edu.spatialindex.quatree.{QTree, SBQTree}
import cs.purdue.edu.spatialindex.rtree.{RTree, Box, Point, Entry}
import cs.purdue.edu.spatialindex.spatialbloomfilter.SBFilter
import org.scalatest.{Matchers, FunSpec}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 6/14/16.
 */
class testSFilter extends FunSpec with Matchers {

  val data = ArrayBuffer.empty[Entry[String]]

  val boxes = ArrayBuffer.empty[Box]

  def readdata =
  {
    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    val files = getListOfFiles("/home/merlin/workspacehadoop/GenerateBoxes/twitter.dat")
    import scala.io.Source
    files.foreach {
      file
      =>
        if(!file.getName.contains(".crc"))
          try{

            for (line <- Source.fromFile(file).getLines()) {
              val x=new WKTReader().read(line)
              val corrds = x.getCoordinates
              val p1 = corrds(0)
              val p = Entry(Point(p1.x.toFloat, p1.y.toFloat), "1")
              data.append(p)
            }

          }catch {
            case e: Exception =>
              println("input format error")
          }

    }

    //read the rectangles
    val rectangles = "/home/merlin/workspacehadoop/GenerateBoxes/boxes.dat"
    for (line <- Source.fromFile(rectangles).getLines()) {
      val x=new WKTReader().read(line)
      val corrds = x.getCoordinates
      val p1 = corrds(0)
      val p2 = corrds(2)
      boxes.append(Box(p1.x.toFloat, p1.y.toFloat, p2.x.toFloat, p2.y.toFloat))
    }

    println("number of tuples "+data.size)
    println("number of boxes "+boxes.size)
  }

  describe("test sfilter for the twitter data")
  {

    readdata
    var b2=System.currentTimeMillis

    val runtime = Runtime.getRuntime
    val mb = 1024*1024
    println("** Used Memory before building R-tree:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    val Rtreeindex=RTree(data)
    println("** Used Memory for the R-tree:  " + (runtime.totalMemory - runtime.freeMemory) / mb)

    println("build rtree index time: "+(System.currentTimeMillis-b2) +" ms")

    b2=System.currentTimeMillis

    var emptyslot=0

    boxes.foreach{
      box=> //println(box.toString)
        if(Rtreeindex.search(box).size==0)
        {
          emptyslot+=1
        }
      //println(rt.search(box).size)
    }
    val rtreeQuerytime=(System.currentTimeMillis-b2)
    println("Rtree based range query time: "+rtreeQuerytime +" ms")
    println("# of empty slot: "+emptyslot)

    println()
    println()
  }

  describe("test quadtree for the twitter data")
  {
    var b2=System.currentTimeMillis
    val runtime = Runtime.getRuntime
    val mb = 1024*1024

    println("** Used Memory before building Q-tree:  " + (runtime.totalMemory - runtime.freeMemory) / mb)

    val Quadtree=QTree(data: _*)

    println("** Used Memory for the Q-tree:  " + (runtime.totalMemory - runtime.freeMemory) / mb)

    println("build qtree index time: "+(System.currentTimeMillis-b2) +" ms")

    b2=System.currentTimeMillis
    var emptyslot=0
    boxes.foreach{
      box=> //println(box.toString)
        if(Quadtree.search(box).size==0)
        {
          emptyslot+=1
        }
    }
    val rtreeQuerytime=(System.currentTimeMillis-b2)

    println("Qtree based range query time: "+rtreeQuerytime +" ms")

    println("# of empty slot: "+emptyslot)

    println()
    println()
  }

  describe("test sfilter for the twitter data")
  {
    /****************************************************/
    var b2=System.currentTimeMillis
    val qtree=new SBQTree(1000)
    qtree.trainSBfilter(data.map(e=>e.geom).toIterator)
    println("train the sbfilter time: "+(System.currentTimeMillis-b2) +" ms")

    val runtime = Runtime.getRuntime
    val mb = 1024
    println("** Used Memory before building sFilter:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    val sbfilter=SBFilter(qtree.getSBFilterV2())
    println("** Used Memory before building sFilter:  " + (runtime.totalMemory - runtime.freeMemory) / mb)

    var timeforQtree=0.0

    var count=0
    boxes.foreach{
      box=>
        //if(sbfilter.searchRectangleWithP(box)<0.2)
        val b2=System.currentTimeMillis
        if(sbfilter.searchRectangle(box)==false)
        {
          count+=1
        }

        timeforQtree+=System.currentTimeMillis-b2
    }
    println("binnary sbfilter time query time: "+(timeforQtree) +" ms")
    println("empty slot: "+count)

  }


}
