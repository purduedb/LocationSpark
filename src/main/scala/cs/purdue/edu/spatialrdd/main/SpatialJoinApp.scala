package cs.purdue.edu.spatialrdd.main

import com.vividsolutions.jts.io.WKTReader
import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.Util
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Try

/**
 * Created by merlin on 6/8/16.
 */
object SpatialJoinApp {

  val usage = """
    Implementation of Spatial Join on Spark
    Usage: spatialjoin --left left_data
                       --right right_data
                       --index the local index for spatial data (default:rtree)
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
        case "--index" :: value :: tail =>
          nextOption(map = map ++ Map('index -> value.toBoolean), list = tail)
        case option :: tail => println("Unknown option " + option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)

    val conf = new SparkConf().setAppName("Test for Spatial JOIN SpatialRDD")
    val spark = new SparkContext(conf)

    val leftFile = options.getOrElse('left, Nil).asInstanceOf[String]
    val rightFile = options.getOrElse('right, Nil).asInstanceOf[String]
    Util.localIndex = options.getOrElse('index, Nil).asInstanceOf[String]

    /** **********************************************************************************/
    //this is for WKT format for the left data points
    val leftpoints = spark.textFile(leftFile).map(x => (Try(new WKTReader().read(x))))
      .filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }
    val leftLocationRDD = SpatialRDD(leftpoints).cache()
    /** **********************************************************************************/

    /** **********************************************************************************/
    val rightData = spark.textFile(rightFile)
    val rightBoxes = rightData.map(x => (Try(new WKTReader().read(x))))
      .filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        val p2 = corrds(2)
        Box(p1.x.toFloat, p1.y.toFloat, p2.x.toFloat, p2.y.toFloat)
    }

    def aggfunction1[K, V](itr: Iterator[(K, V)]): Int = {
      itr.size
    }

    def aggfunction2(v1: Int, v2: Int): Int = {
      v1 + v2
    }

    /** **********************************************************************************/
    val joinresultRdd = leftLocationRDD.rjoin(rightBoxes)(aggfunction1, aggfunction2)

    println("join result size " + joinresultRdd.count())

    spark.stop()

  }

}


//val scheduler=new joinScheduler(indexed,queryboxes)
//val joinresultRdd=scheduler.scheduleRJoin(aggfunction1, aggfunction2)

//joinresultRdd.sortBy(x=>x._2).take(200).foreach(println)
//joinresultRdd.take(10).foreach(println)