package cs.purdue.edu.spatialindex.localSjoinTest

import cs.purdue.edu.spatialindex.rtree._
import cs.purdue.edu.spatialindex.quatree._
import cs.purdue.edu.spatialindex.spatialbloomfilter.qtreeUtil
import org.scalatest.{Matchers, FunSpec}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 5/27/16.
 */
class nestLoopQadtreeJoin  extends FunSpec with Matchers{

  describe("test for nest loop quadtree sjoin on twitter data")
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

    val numberofdatapoints=200000
    val datatree=QTree(data.take(numberofdatapoints).toIterator)

    for(iteartion<-1 to 20)
    {

      val numberofqueries=10000*iteartion

      val queries=data.take(numberofqueries).map{
        case (p:Entry[v])=>
          val r=qtreeUtil.getRandomUniformPoint(3,3)
          (Box(p.geom.x,p.geom.y,p.geom.x+r.x,p.geom.y+r.y))
      }

      var count=0

      var b2=System.currentTimeMillis

      queries.foreach
      {
        case box=>
          count+=datatree.search(box).size
      }

      println("number of query result "+count)

      val queryTime=(System.currentTimeMillis-b2)

      println("number of query: "+ numberofqueries)
      println("number of data size: " + numberofdatapoints)
      println("nest loop range query time for quadtree time: "+queryTime +" ms")
      println("*"*100)

    }

  }
}
