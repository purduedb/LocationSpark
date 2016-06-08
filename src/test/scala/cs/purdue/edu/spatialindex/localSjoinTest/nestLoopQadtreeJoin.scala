package cs.purdue.edu.spatialindex.localSjoinTest

import com.vividsolutions.jts.io.WKTReader
import cs.purdue.edu.spatialindex.rtree._
import cs.purdue.edu.spatialindex.quatree._
import cs.purdue.edu.spatialindex.spatialbloomfilter.qtreeUtil
import org.scalatest.{Matchers, FunSpec}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * Created by merlin on 5/27/16.
 */
class nestLoopQadtreeJoin  extends FunSpec with Matchers{

  def getBoxes(address:String):Iterable[Box]=
  {
    val data=ArrayBuffer.empty[Box]
    import scala.io.Source

    for (line <- Source.fromFile(address).getLines())
    {
      if(Try(new WKTReader().read(line)).isSuccess)
      {
        val ploygan=new WKTReader().read(line)
        val corrds=ploygan.getCoordinates
        val p1=corrds(0)
        val p2=corrds(2)
        data.append( Box(p1.x.toFloat,p1.y.toFloat, p2.x.toFloat,p2.y.toFloat))
      }
    }

    data.toIterable

  }

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
              val p=Entry(Point(arry(1).toFloat, arry(0).toFloat), arry(2))
              data.append(p)
            }
            else if (arry.size==2)
            {
              val p=Entry(Point(arry(1).toFloat, arry(0).toFloat), "xxxx")
              data.append(p)
            }

          }catch
            {
              case e:Exception=>

                println("input format error")
            }
        }
    }



    for(iteartion<-1 to 5)
    {

      val numberofdatapoints=10000*iteartion

      val datatree=QTree(data.take(numberofdatapoints).toIterator)

      val boxes=getBoxes("/home/merlin/workspacehadoop/GenerateBoxes/test.dat")

      var count=0

      var b2=System.currentTimeMillis

      boxes.foreach
      {
        case box=>
          count+=datatree.search(box).size
      }

      println("number of query result "+count)

      val queryTime=(System.currentTimeMillis-b2)

      //println("number of query: "+ numberofqueries)
      println("number of data size: " + numberofdatapoints)
      println("nest loop range query time for quadtree time: "+queryTime +" ms")
      println("*"*100)

    }

  }
}
