package test

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialindex.rtree._
import org.apache.spark.{SparkContext, SparkConf}
import cs.purdue.edu.spatialrdd.impl.{Grid2DPartitionerForBox, Util}


/**
 * Created by merlin on 9/20/15.
 */



object testSpatialRDD
{


  def testForBuildRDD(spark:SparkContext): Unit =
  {

    val numofpoints=1000
    val rdd = spark.parallelize((1 to numofpoints).map(x => (qtreeUtil.getRandomUniformPoint(3000,3000), x)), 9)
    val indexed = SpatialRDD(rdd).cache()

  }
  /**
   * test for put and get function
   */
  def testForPut(srdd:SpatialRDD[Point,Int])=
  {
    val insertpoint=Point(300, 383)
    val indexrdd2=srdd.put(insertpoint,100)
    assert(indexrdd2.get(insertpoint)==Some(100))
  }

  def testForGet[V](indexed:SpatialRDD[Point,V])=
  {
    val insertpoint2=Point(30.40094f,-86.8612f)

    indexed.get(insertpoint2).foreach(println)

    assert(indexed.get(insertpoint2)!=None)

  }

  /**
   * test for multi-put and multi-get
   */
  def testForMultiPutGet(srdd:SpatialRDD[Point,Int])=
  {
    val z = Array(Point(12,12), Point(17,18), Point(20,21))
    val indexrdd3=srdd.multiput(Map(Point(12,12) -> 1, Point(17,18) -> 12, Point(20,21)->100), SumFunction)
    indexrdd3.multiget(z).foreach(println)
  }

  /**
   * test for range query
   */
  def testForRangeQuery[V](srdd:SpatialRDD[Point,V])=
  {
    //val box = Box(2 , 2, 90, 90)

    val searchbox=Box(20.10094f,-86.8612f, 32.41f, -80.222f)

    val rangesearchresult=srdd.rangeFilter(searchbox,(id)=>true)

    rangesearchresult.foreach(println)

  }

  /**
   * test for knn search
   */
  def testForKNNQuery[V](srdd:SpatialRDD[Point,V])=
  {
    val k=20
    val querypoint=Point(30.40094f,-86.8612f)
    //val insertpoint=Point(-17, 18)
    val knnresults=srdd.knnFilter(querypoint,k,(id)=>true)

    knnresults.foreach(println)

  }

  def testForSJOIN[V](srdd:SpatialRDD[Point,V], spark:SparkContext)=
  {
    /**
     * test for the spatial join
     */

    val numpartition=4

    val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangx,numpartition)

    val numofQueries=2

    //val boxes=Array{(Box(0,0,100,100),1); (Box(0,100,1000,1000),2)}
    val boxes=Array{(Box(20.10094f,-86.8612f, 32.41f, -80.222f),2)}

    val queryBoxes=spark.parallelize(boxes,numpartition)

    val transfromQueryRDD=queryBoxes.flatMap{
      case(box:Box,id)=>
        boxpartitioner.getPartitionsForRangeQuery(box).map(p=>(p,box))
    }

    val joinresultRdd=srdd.sjoin(transfromQueryRDD)((k,id)=>id)

    joinresultRdd.foreach(println)
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Test for Spark SpatialRDD")

    val spark = new SparkContext(conf)

    val inputfile=args(0)

    val datardd=spark.textFile(inputfile)

    val locationRDD=datardd.filter((line:String)=>line.split(",").length>=6).map{
      line=>
       val arry=line.split(",")
        (Point(arry(2).toFloat, arry(3).toFloat), arry(5) )
    }

    val indexed = SpatialRDD(locationRDD).cache()

    testForSJOIN(indexed,spark)

    spark.stop()

  }


}

// Declared outside of test suite to avoid closure capture
private object SumFunction extends Function3[Point, Int, Int, Int] with Serializable {
  def apply(id: Point, a: Int, b: Int) = a + b
}