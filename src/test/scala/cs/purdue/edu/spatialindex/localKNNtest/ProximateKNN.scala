package cs.purdue.edu.spatialindex.localKNNtest

import java.io.File

import cs.purdue.edu.spatialanalysis.Kmeans
import cs.purdue.edu.spatialindex.rtree._
import org.scalatest.{FunSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 5/16/16.
 */
class ProximateKNN extends FunSpec with Matchers {

  describe("from the TKDE approximate knn join approach")
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
    val numberOfQueries=20000

    val querypoints=data.take(numberOfQueries).map(e=>e.geom.asInstanceOf[Point]).toIndexedSeq

    /****************************************************************************************/
    var b1=System.currentTimeMillis
    val sampleratio=0.3
    val forkmeansdata=querypoints.take((querypoints.size*sampleratio).toInt)
    val numberofcluster=500

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
        updateHashmap(tmppivot,point)
    }

    def l2distance(x1:Double,y1:Double,x2:Double,y2:Double):Double=
    {
      math.sqrt((x2-x1)*(x2-x1)+(y1-y2)*(y1-y2))
    }

    /**
     * change to range searching
     */

    var bundary=0.0

    val knnBoundary=buf.map{
      case(pivot,datapoints)=>

        var radius=Double.MinValue
        datapoints.foreach
        {
          case p:Point=>
            val tmpdistance=p.distance(pivot.asInstanceOf[Point])
            radius=Math.max(radius,tmpdistance)
        }

        val maxdistance=3*radius
        bundary=bundary+maxdistance
        (datapoints,pivot,maxdistance)

    }.map {
        case(datapoint,pivot,maxdistance)=>

          val querybox=Box(
            (pivot.x-maxdistance).toFloat,
            (pivot.y-maxdistance).toFloat,
            (pivot.x+maxdistance).toFloat,
            (pivot.y+maxdistance).toFloat)

          (querybox.asInstanceOf[Geom], datapoint.toIterable)
      }.toMap

    val queryboxes=knnBoundary.map
    {
      case(box,pts)=>
        Entry(box,"1")
    }

    Constants.MaxEntries=20
    val qboxTree=RTree(queryboxes)


    for(a<-1 until 15)
    {
      b1=System.currentTimeMillis
      val k=10*a
      val sjoinResults=datatree.joinsForKNNjoin(qboxTree, knnBoundary, k)

      var returnsize=0
      sjoinResults.foreach
      {
        case(p,pts)=>
          returnsize+=pts.size
      }

      println("return data size "+ returnsize)

      println("k "+ k+ " time for get the knn result based on approximate range: "+(System.currentTimeMillis-b1) +" ms")
    }








  }

}
