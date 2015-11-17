package cs.purdue.edu.spatialrdd

import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by merlin on 11/16/15.
 */
object SpatialRDDMain {

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

    //println("data size")
    //println(locationRDD.count())

    val indexed = SpatialRDD(locationRDD).cache()

    println("index rdd data size")
    println(indexed.count())

    println("*"*100)

    val searchbox=Box(20.10094f,-86.8612f, 32.41f, -80.222f)

    val rangesearchresult=indexed.rangeFilter(searchbox,(id)=>true)

    //rangesearchresult.foreach(println)

    rangesearchresult.size


    //samplerdd.foreach(println)

    /*val indexed = SpatialRDD(locationRDD).cache()

    val searchbox=Box(20.10094f,-86.8612f, 32.41f, -80.222f)

    val rangesearchresult=indexed.rangeFilter(searchbox,(id)=>true)

    rangesearchresult.foreach(println)*/

    spark.stop()

  }
}
