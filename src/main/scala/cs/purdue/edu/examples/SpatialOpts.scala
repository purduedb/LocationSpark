package cs.purdue.edu.examples

import com.vividsolutions.jts.io.WKTReader
import cs.purdue.edu.spatialindex.rtree.{Box, Entry, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.Util
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
 * Created by merlin on 11/16/15.
 */
object SpatialOpts {

  val usage = """
    Implementation of Spatial Operators on Spark
    Usage: spatial-opt --data input data
                       --index the local index for spatial data (default:rtree)
                       --master master node for this app
                       --help
              """

  def testForTwitterType(spark:SparkContext, inputTwitterFile:String): Unit =
  {
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
  }

  //this class is mainly used for testing the basic spatial operatos over locationRDD

  def main(args: Array[String]) {

    if(args.length==0) println(usage)

    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--help" :: tail =>
          println(usage)
          sys.exit(0)
        case "--data" :: value :: tail =>
          nextOption(map ++ Map('data -> value), tail)
        case "--index" :: value :: tail =>
          nextOption(map = map ++ Map('index -> value), list = tail)
        case option :: tail => println("Unknown option " + option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)

    val inputfile = options.getOrElse('data, Nil).asInstanceOf[String]
    Util.localIndex = options.getOrElse('index, Nil).asInstanceOf[String]

    val conf = new SparkConf().setAppName("Test for Spatial search and knn search operator").setMaster("local[2]")

    val spark = new SparkContext(conf)
    /** **********************************************************************************/
    //this is for WKT format for the left data points, without text information

    var b1 = System.currentTimeMillis
    val leftpoints = spark.textFile(inputfile).map(x => (Try(new WKTReader().read(x))))
      .filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }

    /** **********************************************************************************/

    val LocationRDD = SpatialRDD(leftpoints).cache()
    println(LocationRDD.count())
    val buildtime=System.currentTimeMillis - b1
    println("build index time "+buildtime)

    /** **********************************************************************************/
    /****************************test for range search search *************************************/
    val searchbox=Box(-71.50177f, 42.234618f, -70.9758f, 42.565219f)
    b1 = System.currentTimeMillis
    val rangesearchresult=LocationRDD.rangeFilter(searchbox,(id)=>true)

    println("range query result "+rangesearchresult.size)

    val rangesearchtime=System.currentTimeMillis - b1
    println("*"*100)
    println("range search time "+rangesearchtime)

    /** **********************************************************************************/
    /****************************test for kNN search *************************************/

    val querypoint=Point(-71.50177f, 42.234618f)
    val k=10
    b1 = System.currentTimeMillis
    val knnresults=LocationRDD.knnFilter(querypoint,k,(id)=>true)
    knnresults.foreach(println)
    var knnsearchtime=System.currentTimeMillis - b1
    println("*"*100)
    println("knn search time for k=10 "+knnsearchtime)


    /***************************************************************************************/
    /****************************close the spark ****************************/
    spark.stop()

  }
}

