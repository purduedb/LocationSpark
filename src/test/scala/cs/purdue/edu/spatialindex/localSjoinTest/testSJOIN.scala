package cs.purdue.edu.spatialindex.localSjoinTest

import cs.purdue.edu.spatialindex.rtree._
import cs.purdue.edu.spatialindex.spatialbloomfilter.qtreeUtil
import org.scalatest.{FunSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 5/12/16.
 */
class testSJOIN  extends FunSpec with Matchers  {

  describe("test for the spatial join, and dual tree based approach for small data, correctness checking")
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
    val datatree=RTree(data.take(20000))

    val querybox1=Box(40.720116f,-77.84535f, 45.00757f,-73.55791f)
    val querybox2=Box(40.720116f,-77.84535f, 41.00757f,-74.55791f)
    val querybox3=Box(41.720116f,-77.84535f, 42.00757f,-75.55791f)
    val querybox4=Box(30.720116f,-83.84535f, 31.00757f,-80.55791f)
    val querybox5=Box(40.720116f,-77.84535f, 42.00757f,-73.55791f)
    val querybox6=Box(20.720116f,-73.84535f, 21.00757f,-71.55791f)
    val querybox7=Box(43.720116f,-76.84535f, 42.00757f,-75.55791f)
    val querybox8=Box(38.720116f,-84.84535f, 39.00757f,-83.55791f)

    val boxes=Array(querybox1,querybox2,querybox3,querybox4,
      querybox5,querybox6,querybox7,querybox8)


    println("**********************************************")
    println("********native tree approach base*************")
    println("**********************************************")

    var b1=System.currentTimeMillis

    boxes.foreach
    {
      box=>
        println("box "+box+" "+datatree.search(box).size)
        //datatree.search(box)
    }

    println("native sjoin time: "+(System.currentTimeMillis-b1) +" ms")

    println("**********************************************")
    println("**********dual tree approach base*************")
    println("**********************************************")

    val boxes2=boxes.map
      {
        case(box)=>
          Entry(box,"1")
      }.toIterator

    def aggfunction1[K,V](itr:Iterator[(K,V)]):Int=
    {
      itr.size
    }

    def aggfunction2(v1:Int, v2:Int):Int=
    {
      v1+v2
    }

    b1=System.currentTimeMillis

    Constants.MaxEntries=2
    val boxtree=RTree(boxes2)
    datatree.joins(boxtree)(aggfunction1,aggfunction2).foreach(println)

    println("dual tree based sjoin time: "+(System.currentTimeMillis-b1) +" ms")

    println()

  }

  describe("test for the spatial join, and dual tree based approach for bigger data, effecience checking")
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
    val numofDatapts=200000

    val datatree=RTree(data.take(numofDatapts))

    val numOfQueries=20000;
    val boxes=data.take(numOfQueries).map{
      case x:Entry[String]=>
        val p=x.geom
        val r=qtreeUtil.getRandomUniformPoint(1,1)
        (Box(p.x,p.y,p.x+r.x,p.y+r.y))
    }

    println("**********************************************")
    println("********native tree approach base*************")
    println("**********************************************")

    var b1=System.currentTimeMillis

    boxes.foreach
    {
      box=>datatree.search(box)
    }

    println("native sjoin time: "+(System.currentTimeMillis-b1) +" ms")

    println("**********************************************")
    println("**********dual tree approach base*************")
    println("**********************************************")

    val boxes2=boxes.map
      {
        case(box)=>
          Entry(box,"1")
      }.toIterator

    def aggfunction1[K,V](itr:Iterator[(K,V)]):Int=
    {
      itr.size
    }

    def aggfunction2(v1:Int, v2:Int):Int=
    {
      v1+v2
    }

    b1=System.currentTimeMillis

    Constants.MaxEntries=20
    val boxtree=RTree(boxes2)
    datatree.joins(boxtree)(aggfunction1,aggfunction2)

    println("dual tree based sjoin time: "+(System.currentTimeMillis-b1) +" ms")

    println()
  }
}
