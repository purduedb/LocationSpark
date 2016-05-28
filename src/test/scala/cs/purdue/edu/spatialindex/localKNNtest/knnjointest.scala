package cs.purdue.edu.spatialindex.localKNNtest

import java.io.File

import cs.purdue.edu.spatialindex.rtree._
import org.scalatest.{FunSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Created by merlin on 1/26/16.
 */
class knnjointest extends FunSpec with Matchers {

  describe("knn test based on rtree and sfilling curve") {

    /*info("starting first test")
    it("translates Point(Double,Double) to Long, sort them, and partition those data points") {

      val numberofdata=1000
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

      val querypoints=data.take(numberofdata)

      val resolution = 16
      val sfc = new HilbertCurve2D(resolution)

      val querywithSFvalue=querypoints.map{
        case x:Entry[String]=>
          val p=x.geom
          val svalue= sfc.toIndex(p.y.toDouble,p.x.toDouble)
          (x,svalue)
      }.sortBy(_._2)

      val threshold=50
      //this is the hash map for partition and (pivot point and box)

      val map=mutable.HashMap.empty[Int,(Point,Box)]
      var pid=0

      var max_x=Float.MinValue
      var max_y=Float.MinValue
      var min_x=Float.MaxValue
      var min_y=Float.MaxValue

      /**
       * O(n)
       */
      for(i<- 0 until querywithSFvalue.size)
      {
        val point=querywithSFvalue(i)._1.geom
        max_x=math.max(point.x,max_x)
        max_y=math.max(point.y,max_y)
        min_x=math.min(point.x,min_x)
        min_y=math.min(point.y,min_y)

        //for data in each partition, get the related boundary and center point
        if(i%threshold==0&&i!=0)
        {
          //use two cornner point as the box and central point
          val centerPoint=Point((max_x+min_x)/2, (max_y+min_y)/2)
          val box=Box(min_x,min_y,max_x,max_y)
          map.put(pid,(centerPoint,box))
          pid+=1
           max_x=Float.MinValue
           max_y=Float.MinValue
           min_x=Float.MaxValue
           min_y=Float.MaxValue
        }
      }

     /* map.foreach{
        case(pid,(center,box))=>
          print(pid+" "+center+" "+box)
          println
      }*/

    }*/

    it("pivot based Vgram partition the data set," +
      " then, find its KNN from the dataset")
    {

      val numberofdata=10000
      val k=10
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

      Constants.MaxEntries=400
      val datatree=RTree(data: _*)

      /**********************************************************/
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
      /*val querypoints=data.take(numberofdata).map
      {
        case entry=>
          val r=qtreeUtil.getRandomUniformPoint(1,1)
          Entry(Point(entry.geom.x+r.x, entry.geom.y+r.y), "1")
      }*/

      val querypoints=data.take(numberofdata)
      var b1=System.currentTimeMillis

      //find the pivots
      val numofpivots=10
      var iteration=numofpivots
      val pivots=ArrayBuffer.empty[Entry[String]]

      while(iteration>0)
      {
        val index=Random.nextInt(querypoints.size)
        pivots.append(querypoints(index))
        iteration=iteration-1
      }

      //repartition the data based on the pivot
      querypoints.foreach
      {
        case point=>
          var mindistance=Double.MaxValue
          var tmppivot=pivots(0)

          pivots.foreach
          {
            case pivot=>
              val distance= point.geom.distance(pivot.geom.asInstanceOf[Point])
              if(distance<mindistance)
              {
                mindistance=distance
                tmppivot=pivot
              }
          }
          //move this point
          updateHashmap(tmppivot.geom,point.geom.asInstanceOf[Point])
      }

      //use the repartition data for the knn query
      val knnforpivot=buf.map{
        case(pivot,datapoints)=>
          //knn searching for pivot point
          val knnresult=datatree.nearestK(pivot.asInstanceOf[Point],k)
          //second approach to find the bounary for each point
          (datapoints,knnresult)
      }

      var averagedistance=0.0
      //this is the hash map for partition and (pivot point and box)
      val queryboxes=knnforpivot.map
      {
        case(datapoints,pivots)=>
          datapoints.map
          {
            case qpoint=>
              var maxdistance=Double.MinValue
              pivots.foreach
              {
                case(e)=>
                  maxdistance=math.max(qpoint.distance(e.geom.asInstanceOf[Point]),maxdistance)
              }
              averagedistance=averagedistance+maxdistance
              val querybox= Box((qpoint.x-maxdistance).toFloat,(qpoint.y-maxdistance).toFloat,
                (qpoint.x+maxdistance).toFloat,(qpoint.y+maxdistance).toFloat)
              querybox
          }
      }

      println("boundary "+ averagedistance/numberofdata)
      println("time for get the boundary "+(System.currentTimeMillis-b1) +" ms")

    }

    /*it("find the represent point for each cluster and for each pivot point," +
      " find its KNN from the dataset")
    {

      val numberofdata=10000
      val k=10
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

       Constants.MaxEntries=400
       val datatree=RTree(data: _*)

      val querypoints=data.take(numberofdata)

      var b1=System.currentTimeMillis

      val resolution = 16
      val sfc = new HilbertCurve2D(resolution)

      val querywithSFvalue=querypoints.map{
        case x:Entry[String]=>
          val p=x.geom
          val svalue= sfc.toIndex(p.y.toDouble,p.x.toDouble)
          (x,svalue)
      }.sortBy(_._2)

      val threshold=100
      //this is the hash map for partition and (pivot point and box)

      val map=mutable.HashMap.empty[Int,(Point,Box)]
      var pid=0

      /**
       * O(n)
       */

      var max_x=Float.MinValue
      var max_y=Float.MinValue
      var min_x=Float.MaxValue
      var min_y=Float.MaxValue

      for(i<- 0 until querywithSFvalue.size)
      {
        val point=querywithSFvalue(i)._1.geom
        max_x=math.max(point.x,max_x)
        max_y=math.max(point.y,max_y)
        min_x=math.min(point.x,min_x)
        min_y=math.min(point.y,min_y)

        //for data in each partition, get the related boundary and center point
        if(i%threshold==0&&i!=0)
        {
          //use two cornner point as the box and central point
          val centerPoint=Point((max_x+min_x)/2, (max_y+min_y)/2)
          val box=Box(min_x,min_y,max_x,max_y)
          map.put(pid,(centerPoint,box))
          pid+=1
          max_x=Float.MinValue
          max_y=Float.MinValue
          min_x=Float.MaxValue
          min_y=Float.MaxValue
        }
      }
      println("hcurve preprocessing time "+(System.currentTimeMillis-b1) +" ms")
      //this is for centroid point
      /*for(i<- 0 until querywithSFvalue.size)
      {
        val point=querywithSFvalue(i)._1.geom

        if(i%(threshold/2)==0&&i%threshold!=0&&i!=0)
        {
          val centerPoint=point.asInstanceOf[Point]
          val box=Box(0,0,0,0)
          map.put(pid,(centerPoint,box))
        }
        //for data in each partition, get the related boundary and center point
        if(i%threshold==0&&i!=0)
        {
          //use two cornner point as the box and central point
          //val centerPoint=Point((max_x+min_x)/2, (max_y+min_y)/2)
          pid+=1
        }
      }*/

      /******************************************/
      //knn search for each pivot point

      def l2distance(x1:Double,y1:Double,x2:Double,y2:Double):Double=
      {
        math.sqrt((x2-x1)*(x2-x1)+(y1-y2)*(y1-y2))
      }

      val knnforpivot=map.map{
        case(pid,(pivot,box))=>
          //knn searching for pivot point
          val knnresult=datatree.nearestK(pivot,k)
          //second approach to find the bounary for each point
          (pid,knnresult)
      }

      val queryboxes=ArrayBuffer.empty[(Box,Point)]

      b1=System.currentTimeMillis

      var averagedistance=0.0

      knnforpivot.foreach
      {
        case(pid,results)=>

          val begin=pid*threshold
          for(i<- begin until begin+threshold; if(i<querywithSFvalue.size))
          {
            val querypoint=querywithSFvalue(i)._1.geom

            var maxdistance=Double.MinValue

            results.foreach
            {
              case(e)=>
                maxdistance=math.max(querypoint.distance(e.geom.asInstanceOf[Point]),maxdistance)
            }

            averagedistance=averagedistance+maxdistance
            //println(maxdistance)

            val querybox= Box((querypoint.x-maxdistance).toFloat,(querypoint.y-maxdistance).toFloat,
              (querypoint.x+maxdistance).toFloat,(querypoint.y+maxdistance).toFloat)

            queryboxes.append((querybox,querypoint.asInstanceOf[Point]))
          }
      }

      println(averagedistance/queryboxes.size)
      println("time for get the boundary "+(System.currentTimeMillis-b1) +" ms")

      b1=System.currentTimeMillis

      val boxes=queryboxes.map{
        case(box,point)=>Entry(box,"x")
      }

      val boxtree=RTree(boxes: _*)

      def aggfunction1[K,V](itr:Iterator[(K,V)]):Int=
      {
        itr.size
      }

      def aggfunction2(v1:Int, v2:Int):Int=
      {
        v1+v2
      }

      datatree.joins(boxtree)(aggfunction1,aggfunction2)

      println("time for box join "+(System.currentTimeMillis-b1) +" ms")
    }*/

  }

}


//find the knn boundary
/******************************************/
//this boundary based on the rectangle is too big for the knn
/*var maxdistance=Double.MinValue
knnresult.foreach
{
  case(entry)=>
    val point=entry.geom
    maxdistance= math.max(maxdistance,l2distance(box.x,box.y, point.x,point.y))
    maxdistance=  math.max(maxdistance,l2distance(box.x2,box.y2, point.x,point.y))
    maxdistance= math.max(maxdistance,l2distance(box.x,box.y2, point.x,point.y))
    maxdistance=math.max(maxdistance,l2distance(box.x2,box.y, point.x,point.y))
}
         //println("knn boundary "+maxdistance)
 (
  Box((pivot.x-maxdistance).toFloat,(pivot.y-maxdistance).toFloat,
  (pivot.x+maxdistance).toFloat,(pivot.y+maxdistance).toFloat),pid
  )*/

/*var averageinbox=0.0
boxes.foreach
{
  case(box,pid)=>
    val inside=datatree.search(box).size
    averageinbox=averageinbox+inside
    println(inside)
}
println("return size "+averageinbox)*/