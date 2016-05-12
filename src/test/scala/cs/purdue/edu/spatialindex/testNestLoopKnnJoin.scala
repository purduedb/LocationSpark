package cs.purdue.edu.spatialindex

import java.io.File

import cs.purdue.edu.spatialindex.rtree.{RTree, Constants, Point, Entry}
import org.scalatest.{Matchers, FunSpec}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 5/11/16.
 */
class testNestLoopKnnJoin extends FunSpec with Matchers{



  describe("native nest loop knn join")
  {

    val data=ArrayBuffer.empty[Entry[String]]

    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    val files=getListOfFiles("/home/merlin/workspacescala/spatialspark/001.txt")
    import scala.io.Source
    files.foreach
    {
      file=>
        for (line <- Source.fromFile(file).getLines()) {
          val arry=line.split(",")
          try {

            if(arry.size==3)
            {
              val p=Entry(Point(arry(0).toFloat, arry(1).toFloat), arry(2))
              data.append(p)
            }
            else if (arry.size==2)
            {
              val p=Entry(Point(arry(0).toFloat, arry(1).toFloat), "xxxx")
              data.append(p)
            }

          }catch
            {
              case e:Exception=>

                println("input format error")
            }
        }
    }

    Constants.MaxEntries=200
    val datatree=RTree(data: _*)

    val numberofdata=20000
    val numberofcluster=100

    val k=100

    val querypoints=data.take(numberofdata).map(e=>e.geom.asInstanceOf[Point]).toIndexedSeq

    val b1=System.currentTimeMillis

    querypoints.foreach
    {
      case point=>
        val count=datatree.nearestK(point,k).size
        println(count)
    }

    println("time for get knn result based on nest loop over index "+(System.currentTimeMillis-b1) +" ms")

  }

}
