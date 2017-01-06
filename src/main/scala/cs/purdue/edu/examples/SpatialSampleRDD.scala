package cs.purdue.edu.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by merlin on 11/16/15.
 */
/**
 * this class is used for sampling the spatial data
 */
object SpatialSampleRDD {

  def main(args: Array[String]) {

   // val conf = new SparkConf().setAppName("Test for Spark SpatialRDD").setMaster("local[2]")

    val conf = new SparkConf().setAppName("Sample the open street map data")

    val spark = new SparkContext(conf)

    require(args.length==3)

    val inputfile=args(0)
    val outputfile=args(1)
    val fraction=args(2)

    val datardd=spark.textFile(inputfile)

    datardd.foreach(println)

    /**
     * this is for the open street map data
     */
    val locationRDD=datardd.map{
      line=>
        try {
          val s=line.split("\\s+")
          //(s(1),s(2))
          (s(2)+","+s(1)+","+s(0))
        }catch
          {
            case e:Exception=>
            //println("input format error")
          }
    }.filter(_!=null)


    val samplerdd=locationRDD.sample(false,fraction.toDouble).repartition(350)


    samplerdd.saveAsTextFile(outputfile)

    spark.stop()

  }
}

/**
 * //this is for twitter data
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
*/

//for the openstreetmap
/**
val locationRDD=datardd.map{
      line=>
        try {
          val s=line.split("\\s+")
          //(s(1),s(2))
          (s(1)+","+s(2))
        }catch
          {
            case e:Exception=>
            //println("input format error")
          }
    }.filter(_!=null)
  */