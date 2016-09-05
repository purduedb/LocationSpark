package cs.purdue.edu.spatialrdd.main

import com.vividsolutions.jts.io.WKTReader
import cs.purdue.edu.spatialindex.rtree.Point
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.{Util, knnJoinRDD}
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Try

/**
 * Created by merlin on 6/8/16.
 */
object KnnJoinApp {

  //this class is mainly used for testing the spatial knn join


  val usage = """
    Implementation of Spatial knn Join on Spark
    Usage: spatialjoin --left left_data
                       --right right_data
                       --index the local index for spatial data (default:rtree)
                       --k the K-nearest-neighbor
                       --help
              """

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
        case "--left" :: value :: tail =>
          nextOption(map ++ Map('left -> value), tail)
        case "--right" :: value :: tail =>
          nextOption(map ++ Map('right -> value), tail)
        case "--k" :: value :: tail =>
          nextOption(map ++ Map('k -> value), tail)
        case "--index" :: value :: tail =>
          nextOption(map = map ++ Map('index -> value), list = tail)
        case option :: tail => println("Unknown option " + option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)

    val leftFile = options.getOrElse('left, Nil).asInstanceOf[String]
    val rightFile = options.getOrElse('right, Nil).asInstanceOf[String]
    Util.localIndex = options.getOrElse('index, Nil).asInstanceOf[String]
    val knn=options.getOrElse('k, Nil).toString.toInt

    val conf = new SparkConf().setAppName("App for Spatial Knn JOIN")
    val spark = new SparkContext(conf)

    /************************************************************************************/
    val leftpoints = spark.textFile(leftFile).map(x => (Try(new WKTReader().read(x))))
      .filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }
    val leftLocationRDD = SpatialRDD(leftpoints).cache()
    /************************************************************************************/

    /************************************************************************************/
    val rightpoints = spark.textFile(rightFile).map(x => (Try(new WKTReader().read(x))))
      .filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat))
    }
    /************************************************************************************/

    var b1 = System.currentTimeMillis

    val knnjoin=new knnJoinRDD[Point,String](leftLocationRDD,rightpoints,knn,(id)=>true,(id)=>true)

    val knnjoinresult=knnjoin.rangebasedKnnjoin()

    val tuples=knnjoinresult.map{case(b,v)=>(1,v.size)}.reduceByKey{case(a,b)=>{a+b}}.map{case(a,b)=>b}.collect()

    println("the outer table size: " + rightpoints.count())
    println("the inner table size: " + leftpoints.count())

    println("global index: "+ Util.localIndex+" ; local index: "+ Util.localIndex)
    println("the k value for kNN join: "+knn)
    println("knn join results size: "+tuples(0))
    println("spatial kNN join time: "+(System.currentTimeMillis - b1) +" (ms)")

    spark.stop()

  }

}
