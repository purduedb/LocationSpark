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

  /**
   * test for put and get function
   */
  def testForPut(srdd:SpatialRDD[Point,Int])=
  {
    val insertpoint=Point(300, 383)
    val indexrdd2=srdd.put(insertpoint,100)
    assert(indexrdd2.get(insertpoint)==Some(100))
  }

  def testForGet(srdd:SpatialRDD[Point,Int])=
  {
    val insertpoint2=Point(-17, 18)
    assert(srdd.get(insertpoint2)==None)

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
  def testForRangeQuery(srdd:SpatialRDD[Point,Int])=
  {
    val box = Box(2 , 2, 90, 90)
    val rangesearchresult=srdd.rangeFilter(box,(id)=>true)
    //rangesearchresult.foreach(println)
  }

  /**
   * test for knn search
   */
  def testForKNNQuery(srdd:SpatialRDD[Point,Int])=
  {
    val k=200
    val insertpoint=Point(-17, 18)
    val knnresults=srdd.knnFilter(insertpoint,k,(id)=>true)
  }

  def testForSJOIN(srdd:SpatialRDD[Point,Int])=
  {

  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark SpatialRDD").setMaster("local[2]")

    val spark = new SparkContext(conf)

    spark.textFile("xxxx")

    val numofpoints=10000
    /**
     * test for building a spatialrdd
     */
    val rdd = spark.parallelize((1 to numofpoints).map(x => (qtreeUtil.getRandomUniformPoint(3000,3000), x)), 9)
    val indexed = SpatialRDD(rdd).cache()

    //indexed.foreach(println)

    /**
     * test for the spatial join
     */
    val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangx,9)

    val numofQueries=2

    //val boxes=Array{(Box(0,0,100,100),1); (Box(0,100,1000,1000),2)}
    val boxes=Array{(Box(900,900,1000,1000),2)}
    val queryBoxes=spark.parallelize(boxes,9)

    //val QueryRDD = spark.parallelize((1 to numofQueries).map(x => (qtreeUtil.getRandomRectangle(2,2,2000,2000), x)), 9)

    val transfromQueryRDD=queryBoxes.flatMap{
      case(box:Box,id)=>
        boxpartitioner.getPartitionsForRangeQuery(box).map(p=>(p,box))
    }

    //transfromQueryRDD.foreach(println)

    val joinresultRdd=indexed.sjoin(transfromQueryRDD)((k,id)=>id)

    //println("*"*100)

    joinresultRdd.foreach(println)

    spark.stop()

  }


}

// Declared outside of test suite to avoid closure capture
private object SumFunction extends Function3[Point, Int, Int, Int] with Serializable {
  def apply(id: Point, a: Int, b: Int) = a + b
}