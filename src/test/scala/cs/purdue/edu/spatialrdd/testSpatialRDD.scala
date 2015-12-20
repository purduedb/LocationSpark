package test

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialindex.rtree._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import cs.purdue.edu.spatialrdd.impl.{QtreePartitioner, Grid2DPartitionerForBox, Util}


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
  def testForRangeQuery[V](indexed:SpatialRDD[Point,V])=
  {
    //val box = Box(2 , 2, 90, 90)

    val searchbox=Box(20.10094f,-86.8612f, 30.41f, -81.222f)

    def textcondition[V](z:Entry[V]):Boolean=
    {
      z.value match
      {
        case v:String =>
          val vl=v.toLowerCase()
          (vl.contains("bitches")||vl.contains("bitch")||v.contains("bit"))
      }
    }

    val rangesearchresult=indexed.rangeFilter(searchbox,(id)=>true)

    println(rangesearchresult.size)

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
    val numpartition=srdd.partitions.length

    //val boxes=Array{(Box(0,0,100,100),1); (Box(0,100,1000,1000),2)}
    val boxes=Array(Box(20.10094f,-86.8612f, 30.41f, -81.222f), Box(29.10094f,-83.8612f, 32.41f, -80.222f))

    val queryBoxes=spark.parallelize(boxes,numpartition)

    val joinresultRdd=srdd.sjoin(queryBoxes)((k,id)=>id)

    joinresultRdd.foreach(println)

  }

  def testForRJOIN[V](srdd:SpatialRDD[Point,V], spark:SparkContext)=
  {
    /**
     * test for the spatial join
     */
    val numpartition=srdd.partitions.length

    //val boxes=Array{(Box(0,0,100,100),1); (Box(0,100,1000,1000),2)}
    val boxes=Array(
      Box(20.10094f,-81.8612f, 30.41f, -84.222f), Box(29.10094f,-83.8612f, 32.41f, -80.222f),
      Box(20.10094f,-86.8612f, 30.41f, -81.222f), Box(19.10094f,-83.8612f, 32.41f, -83.222f),
      Box(20.10094f,-96.8612f, 30.41f, -81.222f), Box(19.10094f,-83.8612f, 34.41f, -82.222f),
      Box(20.10094f,-86.8612f, 40.41f, -81.222f), Box(10.10094f,-83.8612f, 43.41f, -84.222f))

    val queryBoxes=spark.parallelize(boxes,numpartition)

    def aggfunction1[K](itr:Iterator[(K,V)]):Int=
    {
      itr.size
    }

    def aggfunction2(v1:Int, v2:Int):Int=
    {
      v1+v2
    }

    val joinresultRdd=srdd.rjoin(queryBoxes)(aggfunction1,aggfunction2)

    println(joinresultRdd.count())

    joinresultRdd.foreach{println}

    /*val reduceresult=joinresultRdd.reduceByKey
    {
      case(itr1,itr2)=>
        (itr1.++(itr2))
    }

    reduceresult.foreach{
      case(b, result:Iterator[(Point,V)])
      =>
        println(b+" "+result.length)
    }*/

    /*joinresultRdd.foreach{
      itr=>
        itr.foreach
        {
          case(b, result:Iterator[(Point,V)])
          =>
            println(b+" "+result.length)
        }
    }*/

  }


 /* def tranformRDDGridPartition[K,V](queriesboxes:RDD[V], numpartition:Int):RDD[(K,V)]={

    val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangx,numpartition)

    queriesboxes.flatMap{
      case(box:Box)=>
        boxpartitioner.getPartitionsForRangeQuery(box).map(p=>(p.asInstanceOf[K],box.asInstanceOf[V]))
    }
  }

  def tranformRDDQuadtreePartition[K,V](boxRDD:RDD[V], partionner:Option[org.apache.spark.Partitioner]):RDD[(K,V)]={
    boxRDD.flatMap{
      case(box:Box)=>
      {
        partionner.getOrElse(None) match
        {
          case qtreepartition:QtreePartitioner[K,V]=>
            qtreepartition.getPointsForSJoin(box).map(p=>(p.asInstanceOf[K],box.asInstanceOf[V]))
        }
      }
    }
  }*/


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Test for Spark SpatialRDD").setMaster("local[2]")

    val spark = new SparkContext(conf)

    val inputfile=args(0)

    val datardd=spark.textFile(inputfile)

    val locationRDD=datardd.map{
      line=>
        val arry=line.split(",")
        try {
          (Point(arry(0).toFloat, arry(1).toFloat), arry(2))
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

    println(indexed.count)

    //testForRangeQuery(indexed)

    //testForSJOIN(indexed,spark)

    testForRJOIN(indexed,spark)

    /*val numpartition=indexed.partitions.length

    val boxes=Array(Box(20.10094f,-86.8612f, 32.41f, -80.222f),Box(23.10094f,-83.8612f, 32.41f, -80.222f))

    val queryBoxes=spark.parallelize(boxes,numpartition)

    val queriesRDD=tranformRDDQuadtreePartition[Point,Box](queryBoxes,indexed.partitioner)

    queriesRDD.foreach(println)

    val joinresultRdd=indexed.sjoins(queriesRDD)((k,id)=>id)

    joinresultRdd.foreach(println)

    val joinresultRdd2=indexed.sjoin(queryBoxes)((k,id)=>id)

    joinresultRdd2.foreach(println)*/

    spark.stop()

  }


}

// Declared outside of test suite to avoid closure capture
private object SumFunction extends Function3[Point, Int, Int, Int] with Serializable {
  def apply(id: Point, a: Int, b: Int) = a + b
}