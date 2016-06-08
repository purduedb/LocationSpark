package cs.purdue.edu.spatialrdd.main

import com.vividsolutions.jts.io.WKTReader
import cs.purdue.edu.spatialindex.rtree.{Entry, Box, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.Util
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
 * Created by merlin on 11/16/15.
 */
object SpatialRDDMain {

  //this class is mainly used for testing the basic spatial operatos over locationRDD

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Test for Basic Operators of SpatialRDD")
    val spark = new SparkContext(conf)

    require(args.length == 2)

    val inputfile = args(0)
    val inputTwitterFile=args(1)

    /** **********************************************************************************/
    //this is for WKT format for the left data points, witout text information
    val leftpoints = spark.textFile(inputfile).map(x => (Try(new WKTReader().read(x))))
      .filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }

    //this parameter for index type
    Util.localIndex = "rtree"

    val LocationRDD = SpatialRDD(leftpoints).cache()
    /** **********************************************************************************/
    /****************************test for kNN search *************************************/
    val k=20
    val querypoint=Point(30.40094f,-86.8612f)
    val knnresults=LocationRDD.knnFilter(querypoint,k,(id)=>true)
    knnresults.foreach(println)

    /** **********************************************************************************/
    /****************************test for range search search *************************************/
    val searchbox=Box(20.10094f,-86.8612f, 32.41f, -80.222f)
    val rangesearchresult=LocationRDD.rangeFilter(searchbox,(id)=>true)
    rangesearchresult.foreach(println)


    /***************************************************************************************/
    /****************************test for spatial textual query ****************************/
    //read data via the witter format
    val TwitterRDD=spark.textFile(inputTwitterFile).map{
      line=>
        val arry=line.split(",")
        //to a lontitude and latitude format
        try {
          (Point(arry(1).toFloat, arry(0).toFloat), arry(2))
        }catch
          {
            case e:Exception=>
          }
    }.map
    {
      case (point:Point,v)=>(point,v)
      case ()=>null
      case _=>null
    }.filter(_!=null)

    val LocationRDDForTwitter = SpatialRDD(TwitterRDD).cache()
    val rangebox=Box(20.10094f,-86.8612f, 32.41f, -80.222f)

    def textPredicate[V](z:Entry[V]):Boolean=
    {
      z.value match
      {
        case v:String =>
          val vl=v.toLowerCase()
          (vl.contains("apple")||vl.contains("google")||v.contains("bad"))
      }
    }

    val spatialTextualResults=LocationRDDForTwitter.rangeFilter(rangebox,textPredicate)
    spatialTextualResults.foreach(println)

    /***************************************************************************************/
    /****************************close the spark ****************************/
    spark.stop()

  }
}

