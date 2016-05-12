package cs.purdue.edu.spatialindex

import java.io.File

import cs.purdue.edu.spatialanalysis.Kmeans
import cs.purdue.edu.spatialindex.rtree._
import org.scalatest.{Matchers, FunSpec}

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

    val numberofdata=20000
    val k=100
    val querypoints=data.take(numberofdata).map(e=>e.geom.asInstanceOf[Point]).toIndexedSeq

    /********************************************************/
    var b1=System.currentTimeMillis
    val sampleratio=0.22
    val forkmeansdata=querypoints.take((querypoints.size*sampleratio).toInt)
    val numberofcluster=100
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
    //repartition the data based on the pivot
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

    //get the boundary for each cluster
    //use the repartition data for the knn query
    var boundary=0.0
    val knnforpivot=buf.map{
      case(pivot,datapoints)=>
        //knn searching for pivot point
        //get the central point rather than the pivot
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
        val knnresult=datatree.nearestK(centralpoint,k,(id)=>true)
        var maxdistance=Double.MinValue

        knnresult.foreach
        {
          case(d,p)=>
            val point=p.geom.asInstanceOf[Point]
            maxdistance= math.max(maxdistance,l2distance(max_x, max_y, point.x,point.y))
            maxdistance= math.max(maxdistance,l2distance(min_x,min_y, point.x,point.y))
            maxdistance= math.max(maxdistance,l2distance(min_x,max_y, point.x,point.y))
            maxdistance= math.max(maxdistance,l2distance(max_x,min_y, point.x,point.y))
        }

        (datapoints,centralpoint,maxdistance)

    }.map
      //.filter(_._3<10).map
      {
        case(datapoint,pivot,maxdistance)=>
          val querybox=Box((pivot.x-maxdistance).toFloat,(pivot.y-maxdistance).toFloat,
            (pivot.x+maxdistance).toFloat,(pivot.y+maxdistance).toFloat)
          (querybox, datapoint.toIterable)

      }.toMap

    val queryboxes=knnforpivot.map
    {
      case(box,pts)=>
        Entry(box,"1")
    }

    b1=System.currentTimeMillis

    val boxtree=RTree(queryboxes)

    def filter[K,V](K:K,V:V):Boolean=
    {
      true
    }

    def aggfunction1[K,V](itr:Iterator[(K,V)]):Int=
    {
      itr.size
    }

    def aggfunction2(v1:Int, v2:Int):Int=
    {
      v1+v2
    }

    datatree.joins(boxtree)(aggfunction1,aggfunction2)

    println("time for get the knn result "+(System.currentTimeMillis-b1) +" ms")

  }

}
