package cs.purdue.edu.spatialindex.localKNNtest

import java.io.File

import cs.purdue.edu.spatialindex.rtree._
import org.scalatest.{FunSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 5/12/16.
 */
class SFcurveBasedKNNJoin extends FunSpec with Matchers {

  describe("space filling curve based approach")
  {

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

    val resolution = 16
    val sfc = new HilbertCurve2D(resolution)

    for(a<-1 until 15)
    {

      val numberOfQueries = 10000*a

      val k=10

      val querypoints = data.take(numberOfQueries).toIndexedSeq

      val b1 = System.currentTimeMillis

      val threshold=50

      val querywithSFvalue=querypoints.map{
        case g=>
          val x=g.geom
          val svalue= sfc.toIndex(x.y.toDouble,x.x.toDouble)
          (x,svalue)
      }.sortBy(_._2)


      //this is the hash map for partition and (pivot point and box)

      val map=mutable.HashMap.empty[Geom,Iterable[Point]]

      var max_x=Float.MinValue
      var max_y=Float.MinValue
      var min_x=Float.MaxValue
      var min_y=Float.MaxValue

      var c1=0.0
      var c2=0.0
      var pid=0

      def l2distance(x1:Double,y1:Double,x2:Double,y2:Double):Double=
      {
        math.sqrt((x2-x1)*(x2-x1)+(y1-y2)*(y1-y2))
      }

      for(i<- 0 until querywithSFvalue.size)
      {

        val point=querywithSFvalue(i)._1
        c1+=point.x
        c2+=point.y

        max_x=math.max(point.x,max_x)
        max_y=math.max(point.y,max_y)
        min_x=math.min(point.x,min_x)
        min_y=math.min(point.y,min_y)

        //for data in each partition, get the related boundary and center point
        if(i%threshold==0&&i!=0)
        {
          //use two cornner point as the box and central point
          val centerPoint=Point((c1/threshold).toFloat,(c2/threshold).toFloat)

          val knnresult=datatree.nearestK(centerPoint,k)
          //second approach to find the bounary for each point
          var maxdistance=Double.MinValue
          //get the knn boundary
          knnresult.foreach
          {
            case(p)=>
              val point=p.geom
              maxdistance= math.max(maxdistance,l2distance(max_x, max_y, point.x,point.y))
              maxdistance= math.max(maxdistance,l2distance(min_x,min_y, point.x,point.y))
              maxdistance= math.max(maxdistance,l2distance(min_x,max_y, point.x,point.y))
              maxdistance= math.max(maxdistance,l2distance(max_x,min_y, point.x,point.y))
          }

          val querybox= Box((centerPoint.x-maxdistance).toFloat,(centerPoint.y-maxdistance).toFloat,
            (centerPoint.x+maxdistance).toFloat,(centerPoint.y+maxdistance).toFloat)

          val points=querywithSFvalue.slice(pid*threshold,(pid+1)*threshold).map{case(point,d)=>point.asInstanceOf[Point]}

          map.put(querybox,points.toIterable)

          max_x=Float.MinValue
          max_y=Float.MinValue
          min_x=Float.MaxValue
          min_y=Float.MaxValue
          c1=0
          c2=0
          pid+=1
        }
      }

      val boxes=map.map{
        case(box,points)=>Entry(box,"x")
      }

      val qboxTree=RTree(boxes)

      val sjoinResults=datatree.joinsForKNNjoin(qboxTree, map.toMap, k)

      var returnsize=0
      sjoinResults.foreach
      {
        case(p,pts)=>
          returnsize+=pts.size
      }

      println("return data size "+ returnsize)

      //println("numberOfQueries "+ numberOfQueries+ " time for get the knn result based on hbertcurve: "+(System.currentTimeMillis-b1) +" ms")
      println("k: "+ k+ "| query points "+numberOfQueries+" |time for get the knn result based on hbertcurve: "+(System.currentTimeMillis-b1) +" ms")

    }


  }
}
