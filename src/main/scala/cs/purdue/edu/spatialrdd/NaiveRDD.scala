package cs.purdue.edu.spatialrdd

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import cs.purdue.edu.spatialrdd.impl.{QtreePartitioner, Grid2DPartitionerForBox, Grid2DPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext, SparkContext, SparkConf}

import scala.reflect.ClassTag

/**
 * Created by merlin on 11/17/15.
 */
object NaiveRDD {

  def main(args: Array[String]) {

    //val conf = new SparkConf().setAppName("Test for Spark SpatialRDD").setMaster("local[2]")

    val conf = new SparkConf().setAppName("Test for Spark SpatialRDD")

    val spark = new SparkContext(conf)

    require(args.length==2)

    val inputfile=args(0)
    val outputfile=args(1)

    val datardd=spark.textFile(inputfile)

    val locationRDD=datardd.map{
      line=>
        val arry=line.split(",")
        try {
          (Point(arry(0).toFloat, arry(1).toFloat), arry(2))
        }catch
          {
            case e:Exception=>
            //println("input format error")
          }
    }.map
    {
      case (point:Point,v)=>(point,v)
      case ()=>null
      case _=>null
    }.filter(_!=null)


    val quadtreePartitioner=new QtreePartitioner(locationRDD.partitions.length,0.001f,locationRDD)

    val indexed = locationRDD.map{
      case(point,v)=>
        (quadtreePartitioner.getPartition(point),(point,v))
    }

    val queryrdd=locationRDD.sample(false,0.0001)

    println(queryrdd.count)

    val queryboxes=queryrdd.map{
      case (p:Point,v)=>
        val r=qtreeUtil.getRandomUniformPoint(3,3)
        (Box(p.x,p.y,p.x+r.x,p.y+r.y))
    }

    val queryboxRDD=queryboxes.flatMap {
      case (box: Box) => {
        quadtreePartitioner.quadtree.getPIDforBox(box).map(pid => (pid, box))
      }
    }

    val result=indexed.join(queryboxRDD).filter
    {
      case(pid,((po:Point,value),b:Box))=>
        b.contains(po)
      case _=>false
    }

    println(result.count)
    //indexed.partitions.foreach{case(p:Partition)=>}

    /*def sumfunction[V](iterator: Iterator[V])=
    {
      println(iterator.size)
    }

    println("data in each partition")
    indexed.foreachPartition(sumfunction)

    val box=Box(20.10094f,-86.8612f,32.41f, -80.222f)

    val rangeresult=indexed.filter{
      case((p:Point,value))=>
        box.contains(p)
      case _=>true
    }

    println("*"*10)
    println(rangeresult.count())*/
    //indexed.filterByRange(Point(20.10094f,-86.8612f), Point(32.41f, -80.222f))

    spark.stop()

  }

}
