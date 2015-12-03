package cs.purdue.edu.scheduler

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.reflect.ClassTag

/**
 * Created by merlin on 11/30/15.
 */
object testScheduler {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test for Spark SpatialRDD scheduler").setMaster("local[2]")

    val spark = new SparkContext(conf)

    val inputfile=args(0)

    val numpartition=4

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

    val indexed = SpatialRDD(locationRDD).cache()

    val querylocation=locationRDD.sample(false,0.9)

    val queryRDD=querylocation.map{
      case (p:Point,v)=>
        val r=qtreeUtil.getRandomUniformPoint(3,3)
        (Box(p.x,p.y,p.x+r.x,p.y+r.y))
    }

    val scheduler=new joinScheduler(indexed,queryRDD)

    val scheduleJoinRDD=scheduler.scheduleJoin()
    //val joinresult=indexed.sjoin(queryRDD)((k,id)=>id)
    println(scheduleJoinRDD.count())
    //println(joinresult.count())

  }

}
