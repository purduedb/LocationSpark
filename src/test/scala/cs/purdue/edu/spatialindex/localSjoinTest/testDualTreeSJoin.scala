package cs.purdue.edu.spatialindex.localSjoinTest

import cs.purdue.edu.spatialindex.rtree._
import cs.purdue.edu.spatialindex.spatialbloomfilter.qtreeUtil
import org.scalatest.{Matchers, FunSpec}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 5/27/16.
 */
class testDualTreeSJoin extends FunSpec with Matchers {

  describe("test for nest loop rtree sjoin on twitter data")
  {
    val data=ArrayBuffer.empty[Entry[String]]

    import java.io.File

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
    val numberofdatapoints=20000
    val datatree=RTree(data.take(numberofdatapoints))

    for(interation<- 1 to 10)
    {
      val numberofqueries=10000*interation

      val queries=data.take(numberofqueries).map{
        case (p:Entry[v])=>
          val r=qtreeUtil.getRandomUniformPoint(3,3)
          Entry((Box(p.geom.x,p.geom.y,p.geom.x+r.x,p.geom.y+r.y)), "1")
      }

      var b2=System.currentTimeMillis

      val boxtree=RTree(queries)

      val buildindexTime=(System.currentTimeMillis-b2)

      var count=0

      def aggfunction1[K,V](itr:Iterator[(K,V)]):Int=
      {
        itr.size
      }

      def aggfunction2(v1:Int, v2:Int):Int=
      {
        v1+v2
      }

      b2=System.currentTimeMillis

      datatree.joins(boxtree)(aggfunction1,aggfunction2)

      val queryTime=(System.currentTimeMillis-b2)

      println("number of query: "+ numberofqueries)
      println("number of data size: " + numberofdatapoints)
      println("dual tree indexing over boxes time: "+buildindexTime +" ms")
      println("dual tree range query time: "+queryTime +" ms")
      println("*"*100)

    }


  }


}
