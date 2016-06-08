package cs.purdue.edu.spatialindex.localKNNtest

import java.io.File

import cs.purdue.edu.spatialindex.rtree.{Constants, Entry, Point, RTree}
import org.scalatest.{FunSpec, Matchers}

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

    //println("data size "+data.size)
    Constants.MaxEntries=200
    val datatree=RTree(data: _*)

    val p1=Point(40.720116f,-77.84535f)
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
    }


    val numberofdata=20000


    val querypoints=data.take(numberofdata).map(e=>e.geom.asInstanceOf[Point]).toIndexedSeq

   // val querypoint1=Point(40.720116f,-77.84535f)
    //val querypoint2=Point(40.730116f,-77.85535f)
    //val querypoints=Array(querypoint1,querypoint2)

    for( a <- 1 to 15)
    {

      val b1=System.currentTimeMillis

      val k=10*a
      querypoints.foreach
      {
        case point=>
          val count=datatree.nearestK(point,k).size
        //println(count)
      }

      println("ksize "+k+" time for get knn result based on nest loop over index "+(System.currentTimeMillis-b1) +" ms")
    }


  }

}
