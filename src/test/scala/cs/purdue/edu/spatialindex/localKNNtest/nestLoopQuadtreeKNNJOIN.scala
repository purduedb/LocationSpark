package cs.purdue.edu.spatialindex.localKNNtest

import java.io.File

import cs.purdue.edu.spatialindex.quatree.QTree
import cs.purdue.edu.spatialindex.rtree.{RTree, Constants, Point, Entry}
import org.scalatest.{Matchers, FunSpec}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 5/28/16.
 */
class nestLoopQuadtreeKNNJOIN extends FunSpec with Matchers {


  describe("native nest loop knn over quadtree join") {

    val data = ArrayBuffer.empty[Entry[String]]

    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    val files = getListOfFiles("/home/merlin/workspacescala/spatialspark/001.txt")
    import scala.io.Source
    files.foreach {
      file =>
        for (line <- Source.fromFile(file).getLines()) {
          val arry = line.split(",")
          try {

            if (arry.size == 3) {
              val p = Entry(Point(arry(0).toFloat, arry(1).toFloat), arry(2))
              data.append(p)
            }
            else if (arry.size == 2) {
              val p = Entry(Point(arry(0).toFloat, arry(1).toFloat), "xxxx")
              data.append(p)
            }

          } catch {
            case e: Exception =>

              println("input format error")
          }
        }
    }

    //println("data size "+data.size)
    val datatree = QTree(data: _*)

    /*val p1=Point(40.720116f,-77.84535f)
    val p2=Point(42.720116f,-72.84535f)
    val p3=Point(43.720116f,-74.84535f)
    val p4=Point(44.720116f,-75.84535f)
    val points=Array(p1, p2, p3,p4)
    val k=20
    points.foreach {
      case point =>
        val rets=datatree.nearestK(point, k)
        rets.foreach(println)
        println("x"*100)
    }*/

    for (a <- 1 to 12) {

      val k = 10

      val numberofdata = 20000*a

      val querypoints = data.take(numberofdata).map(e => e.geom.asInstanceOf[Point]).toIndexedSeq

      val b1 = System.currentTimeMillis

      querypoints.foreach {
        case point =>
          val count = datatree.nearestK(point, k).size
        //println(count)
      }

      println("quer data size "+numberofdata)
      println("ksize " + k + " time for get knn result based on nest loop over index " + (System.currentTimeMillis - b1) + " ms")
      println("x"*100)
    }

  }

}