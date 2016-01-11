package cs.purdue.edu.spatialrdd.main

import cs.purdue.edu.scheduler.joinScheduler
import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by merlin on 11/16/15.
 */
object SpatialRDDMain {

//this class is mainly used for testing the spatial range join performance
  def main(args: Array[String]) {

    //val conf = new SparkConf().setAppName("Test for Spark SpatialRDD").setMaster("local[2]")

    val conf = new SparkConf().setAppName("Test for Spark SpatialRDD Sjoin")

    val spark = new SparkContext(conf)

    require(args.length==2)

    val inputfile=args(0)
    val ratio=args(1).toFloat

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
    /************************************************************************************/

    /************************************************************************************/
    val queryrdd=locationRDD.sample(false,ratio)

    val queryboxes=queryrdd.map{
      case (p:Point,v)=>
        val r=qtreeUtil.getRandomUniformPoint(3,3)
        (Box(p.x,p.y,p.x+r.x,p.y+r.y))
    }


    def aggfunction1[K,V](itr:Iterator[(K,V)]):Int=
    {
      itr.size
    }

    def aggfunction2(v1:Int, v2:Int):Int=
    {
      v1+v2
    }


    //val scheduler=new joinScheduler(indexed,queryboxes)
    //val joinresultRdd=scheduler.scheduleRJoin(aggfunction1, aggfunction2)

    val joinresultRdd=indexed.rjoin(queryboxes)(aggfunction1,aggfunction2)
    println("join result size "+joinresultRdd.count())

    spark.stop()

  }
}

//println("*"*100)
//println("query box size: " +queryboxes.count())

/*val boxes=Array(
  Box(20.10094f,-81.8612f, 30.41f, -84.222f), Box(29.10094f,-83.8612f, 32.41f, -80.222f),
  Box(20.10094f,-86.8612f, 30.41f, -81.222f), Box(19.10094f,-83.8612f, 32.41f, -83.222f),
  Box(20.10094f,-96.8612f, 30.41f, -81.222f), Box(19.10094f,-83.8612f, 34.41f, -82.222f),
  Box(20.10094f,-86.8612f, 40.41f, -81.222f), Box(10.10094f,-83.8612f, 43.41f, -84.222f))

val queryBoxes=spark.parallelize(boxes,numPartition)*/

/**
 *  println("partition summary for data")

    def getPartitionSize[K : ClassTag](rdd: RDD[K]): (Array[(Int, Int)]) = {
      // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
      val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
        Iterator((idx, iter.size))
      }.collect()
      sketched
    }
 */


/* val numPartition=indexed.partitions.length

 val boxes=Array(Box(17.10094f,-86.8612f, 18.41f, -80.222f), Box(13.10094f,-87.8612f, 14.41f, -83.222f),
   Box(17.10094f,-87.8612f, 18.41f, -84.222f),Box(13.10094f,-87.8612f, 28.41f, -85.222f),
   Box(16.10094f,-88.8612f, 19.41f, -87.222f), Box(23.10094f,-81.8612f, 23.41f, -82.222f))

 val queryBoxeRDD=spark.parallelize(boxes,numPartition)
 val joinresultRdd2=indexed.sjoin(queryBoxeRDD)((k,id)=>id)

 println("join for small data result")
 println(joinresultRdd2.count())*/


//val joinresultRdd=indexed.sjoin(queryboxes)((k,id)=>id)

//val boxes=Array{(Box(0,0,100,100),1); (Box(0,100,1000,1000),2)}
/*val boxes2=Array(Box(20.10094f,-86.8612f, 32.41f, -80.222f), Box(23.10094f,-87.8612f, 28.41f, -83.222f),
  Box(27.10094f,-87.8612f, 28.41f, -84.222f),Box(23.10094f,-87.8612f, 28.41f, -85.222f),
  Box(25.10094f,-88.8612f, 28.41f, -87.222f), Box(20.10094f,-81.8612f, 21.41f, -82.222f))

val queryBoxeRDD2=spark.parallelize(boxes,numPartition)

val joinresultRdd=indexed.sjoin(queryBoxeRDD2)((k,id)=>id)

println(joinresultRdd.count())*/

//println("partition summary for query")
//getPartitionSize(queryboxes).foreach(println)



//joinresultRdd.foreach(println)

//rangesearchresult.foreach(println)

//println("data size")
//println(locationRDD.count())

/*val indexed = SpatialRDD(locationRDD).cache()

println("index rdd data size")
println(indexed.count())

println("*"*100)

val k=20
val querypoint=Point(30.40094f,-86.8612f)
val knnresults=indexed.knnFilter(querypoint,k,(id)=>true)

println("result for the knn search k="+k)
println(knnresults.size)*/


//samplerdd.foreach(println)

/*val indexed = SpatialRDD(locationRDD).cache()

val searchbox=Box(20.10094f,-86.8612f, 32.41f, -80.222f)

val rangesearchresult=indexed.rangeFilter(searchbox,(id)=>true)

rangesearchresult.foreach(println)*/

/*val indexed = SpatialRDD(locationRDD).cache()

val numpartition=indexed.partitions.size

val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangx,numpartition)

val transfromQueryRDD=queryboxes.flatMap{
  case(box:Box)=>
    boxpartitioner.getPartitionsForRangeQuery(box).map(p=>(p,box))
}.cache()

val joinresultRdd=indexed.sjoin(transfromQueryRDD)((k,id)=>id)

println("join size is ")
joinresultRdd.count()*/


//transfromQueryRDD.foreach(println)

//val boxes=Array{(Box(0,0,100,100),1); (Box(0,100,1000,1000),2)}
//val boxes=Array{(Box(20.10094f,-86.8612f, 32.41f, -80.222f),2)}

//val queryBoxes=spark.parallelize(boxes,numpartition)


//joinresultRdd.foreach(println)