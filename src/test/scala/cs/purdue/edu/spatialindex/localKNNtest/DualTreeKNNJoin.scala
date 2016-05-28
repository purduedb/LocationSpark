package cs.purdue.edu.spatialindex.localKNNtest

import java.io.File

import cs.purdue.edu.spatialindex.rtree._
import org.scalatest.{FunSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 5/13/16.
 */
class DualTreeKNNJoin extends FunSpec with Matchers {

  describe("dual tree approach for knn join") {
    /** ******************************************************/
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

    Constants.MaxEntries = 200
    val datatree = RTree(data: _*)

    /** **************************************************************************************/
    val numberOfQueries = 2
    val k = 10
    //val querypoints = data.take(numberOfQueries).map(e => e.geom.asInstanceOf[Point]).toIndexedSeq

    val querypoint1=Point(40.720116f,-77.84535f)
    val querypoint2=Point(40.730116f,-77.85535f)

    val querypoints=Array(querypoint1,querypoint2)

    val queryboxes=querypoints.map(pt=>Entry(pt,"1"))

    Constants.MaxEntries=20

    val qboxTree=RTree(queryboxes)

    def f1(k:Point):Boolean=
    {
      true
    }

    def f2(v:Any):Boolean=
    {
      true
    }

    var b1=System.currentTimeMillis

    datatree.knnjoin(qboxTree,k)(f1, f2)

    println("time for knn search based on dual tree"+(System.currentTimeMillis-b1) +" ms")
  }

}
