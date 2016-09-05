package cs.purdue.edu.spatialrdd.main

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by merlin on 6/7/16.
 */
object ToWKT {

  def main(args: Array[String]) {

     val conf = new SparkConf().setAppName("transform the twitter data into Well know Text format").setMaster("local[2]")

    val spark = new SparkContext(conf)

    require(args.length==2)

    val inputfile=args(0)
    val outputfile=args(1)
    val datardd=spark.textFile(inputfile)

    //datardd.foreach(println)

    /**
     * this is for the open street map data
     */
    val data=datardd.map{
      line=>
        try {
          val s=line.split(",")
          //to longitude/latitude format
          "POINT"+"("+s(1)+" "+s(0)+")"
        }catch
          {
            case e:Exception=>
            //println("input format error")
          }
    }.filter(s=>s.toString.contains("POINT"))

    data.saveAsTextFile(outputfile)

    spark.stop()

  }

}
