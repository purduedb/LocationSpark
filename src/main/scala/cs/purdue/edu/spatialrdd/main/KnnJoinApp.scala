package cs.purdue.edu.spatialrdd.main

import cs.purdue.edu.spatialindex.rtree.Point
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.knnJoinRDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by merlin on 6/8/16.
 */
object KnnJoinApp {

  //this class is mainly used for testing the spatial knn join

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Test for Spark SpatialRDD knnjoin")
    val spark = new SparkContext(conf)

    require(args.length==3)

    val inputfile=args(0)
    val ratio=args(1).toFloat
    val knn=args(2).toInt

    val datardd=spark.textFile(inputfile)

    val locationRDD=datardd.map{
      line=>
        val arry=line.split(",")
        try {
          (Point(arry(1).toFloat, arry(0).toFloat), arry(2))
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

    /************************************************************************************/

    /************************************************************************************/
    val knnqueryRDD=locationRDD.sample(false,ratio).map{
      case (p:Point,v)=>
        p
    }
    def f1(k:Point):Boolean=
    {
      true
    }
    def f2(v:Any):Boolean=
    {
      true
    }

    knnqueryRDD.checkpoint()

    val knnjoin=new knnJoinRDD(indexed,knnqueryRDD,knn,f1,f2)

    val knnjoinresult=knnjoin.rangebasedKnnjoin()

    println("knnjoinresult "+knnjoinresult.count())

    spark.stop()

  }

}
