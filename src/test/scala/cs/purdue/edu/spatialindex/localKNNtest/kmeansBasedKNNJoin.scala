package cs.purdue.edu.spatialindex.localKNNtest

import java.io.File

import cs.purdue.edu.spatialanalysis.Kmeans
import cs.purdue.edu.spatialindex.rtree._
import org.scalatest.{FunSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 5/11/16.
 */
class kmeansBasedKNNJoin extends FunSpec with Matchers {

  describe("the approach in the locationSpark paper")
  {

    /********************************************************/
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

    /****************************************************************************************/

    for(a<-1 until 15)
    {

    val numberOfQueries=10000*a

    val querypoints=data.take(numberOfQueries).map(e=>e.geom.asInstanceOf[Point]).toIndexedSeq

    /****************************************************************************************/
    var b1=System.currentTimeMillis
    val sampleratio=0.3
    val forkmeansdata=querypoints.take((querypoints.size*sampleratio).toInt)
    val numberofcluster=200

    val kcluster=new Kmeans(forkmeansdata, Kmeans.euclideanDistance, 0.0001, 10, false)
    val (c1,pivots)=kcluster.run(numberofcluster,1)
    println("kmeans run time "+(System.currentTimeMillis-b1) +" ms")

    /*************************************************************/

    val buf = mutable.HashMap.empty[Geom,ArrayBuffer[Point]]
    def updateHashmap(key:Geom,value:Point)=
    {
      try {
        if(buf.contains(key))
        {
          val tmp1=buf.get(key).get
          tmp1.append(value)
          buf.put(key,tmp1)
        }else
        {
          val tmp1=new ArrayBuffer[Point]
          tmp1.append(value)
          buf.put(key,tmp1)
        }

      }catch
        {
          case e:Exception=>
            println("out of memory for appending new value to the sjoin")
        }
    }

    /**********************************************************/
    b1=System.currentTimeMillis

    querypoints.foreach
    {
      case point=>
        var mindistance=Double.MaxValue
        var tmppivot=pivots(0)
        pivots.foreach
        {
          case pivot=>
            val distance= point.distance(pivot)
            if(distance<mindistance)
            {
              mindistance=distance
              tmppivot=pivot
            }
        }
        //move this point
        updateHashmap(tmppivot,point)
    }

    def l2distance(x1:Double,y1:Double,x2:Double,y2:Double):Double=
    {
      math.sqrt((x2-x1)*(x2-x1)+(y1-y2)*(y1-y2))
    }

    /**
     * change to range searching
     */

    //option1: use the max knn distance bound, and this will have limit number of rectangels, but this bring overhead
    //for larget amount of data points in each rectangle boundary, and the overhead to get the knn for point
    //in each boundary is too high.

    var bundary=0.0

    val k=10

    val knnBoundary=buf.map{
      case(pivot,datapoints)=>
        var c1=0.0
        var c2=0.0
        var max_x=Float.MinValue
        var max_y=Float.MinValue
        var min_x=Float.MaxValue
        var min_y=Float.MaxValue

        datapoints.foreach
        {
          case p:Point=>
            c1=p.x+c1
            c2=p.y+c2
            max_x=math.max(p.x,max_x)
            max_y=math.max(p.y,max_y)
            min_x=math.min(p.x,min_x)
            min_y=math.min(p.y,min_y)
        }

        val centralpoint=Point((c1/datapoints.size).toFloat,(c2/datapoints.size).toFloat)
        //knn searching for pivot point
        val knnresult=datatree.nearestK(centralpoint,k,(id)=>true)

        //var maxdistance=Double.MinValue
        var maxdistance=Double.MinValue
        //get the knn boundary
        knnresult.foreach
        {
          case(d,p)=>
            val point=p.geom.asInstanceOf[Point]
            maxdistance= math.max(maxdistance,l2distance(max_x, max_y, point.x,point.y))
            maxdistance= math.max(maxdistance,l2distance(min_x,min_y, point.x,point.y))
            maxdistance= math.max(maxdistance,l2distance(min_x,max_y, point.x,point.y))
            maxdistance= math.max(maxdistance,l2distance(max_x,min_y, point.x,point.y))
        }
        (datapoints,pivot,maxdistance)

    }.map {
        case(datapoint,pivot,maxdistance)=>

          val querybox=Box(
            (pivot.x-maxdistance).toFloat,
            (pivot.y-maxdistance).toFloat,
            (pivot.x+maxdistance).toFloat,
            (pivot.y+maxdistance).toFloat
            )
          (querybox.asInstanceOf[Geom], datapoint.toIterable)
      }.toMap

    val queryboxes=knnBoundary.map
    {
      case(box,pts)=>
        Entry(box,"1")
    }

    Constants.MaxEntries=20
    val qboxTree=RTree(queryboxes)

      b1=System.currentTimeMillis
      val sjoinResults=datatree.joinsForKNNjoin(qboxTree, knnBoundary, k)

      var returnsize=0
      sjoinResults.foreach
      {
        case(p,pts)=>
          returnsize+=pts.size
      }

      println("return data size "+ returnsize)
      println("k "+ k+ " time for get the knn result based on kmeans: "+(System.currentTimeMillis-b1) +" ms")

    }

  }

}


/* var queryboxes=ArrayBuffer.empty[Box]

//option 2:

 buf.foreach {
   case (pivot, querypoints) =>
     val knnresult=datatree.nearestK(pivot.asInstanceOf[Point],k,(id)=>true)
     querypoints.foreach
     {
       case q:Point=>
         var maxdistance=0.0
         knnresult.foreach
         {
           case(d, dpt)=>
             maxdistance=Math.max(maxdistance,q.distance(dpt.geom.asInstanceOf[Point]))
         }

         val querybox=Box(
           (q.x-maxdistance).toFloat,
           (q.y-maxdistance).toFloat,
           (q.x+maxdistance).toFloat,
           (q.y+maxdistance).toFloat)

         queryboxes+=querybox
     }

 }*/