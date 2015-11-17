package cs.purdue.edu.spatialrdd

import cs.purdue.edu.spatialindex.rtree.Point
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by merlin on 11/16/15.
 */
object SpatialSampleRDD {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Test for Spark SpatialRDD").setMaster("local[2]")

    //val conf = new SparkConf().setAppName("Test for Spark SpatialRDD")

    val spark = new SparkContext(conf)

    require(args.length==3)

    val inputfile=args(0)
    val outputfile=args(1)
    val fraction=args(2)

    val datardd=spark.textFile(inputfile)

    val locationRDD=datardd.map{
      line=>
        val arry=line.split(",")
        try {
          (arry(2)+","+arry(3)+","+arry(5))
        }catch
          {
            case e:Exception=>
            //println("input format error")
          }
    }.filter(_!=null)

    println("data size")
    println(locationRDD.count())

    val samplerdd=locationRDD.sample(false,fraction.toDouble)

    println("sample data size")
    println(samplerdd.count())

    samplerdd.saveAsTextFile(outputfile)

    //samplerdd.foreach(println)

    /*val indexed = SpatialRDD(locationRDD).cache()

    val searchbox=Box(20.10094f,-86.8612f, 32.41f, -80.222f)

    val rangesearchresult=indexed.rangeFilter(searchbox,(id)=>true)

    rangesearchresult.foreach(println)*/

    spark.stop()

  }
}
