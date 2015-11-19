package cs.purdue.edu.spatialrdd

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree.{Entry, Box, Point}
import cs.purdue.edu.spatialrdd.impl.Grid2DPartitionerForBox
import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by merlin on 11/16/15.
 */
object SpatialRDDMain {

  def main(args: Array[String]) {

    //val conf = new SparkConf().setAppName("Test for Spark SpatialRDD").setMaster("local[2]")

    val conf = new SparkConf().setAppName("Test for Spark SpatialRDD")

    val spark = new SparkContext(conf)

    require(args.length==2)

    val inputfile=args(0)
    val outputfile=args(1)

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

    println("data size")
    println(locationRDD.count())

    val indexed = SpatialRDD(locationRDD).cache()

    val searchbox=Box(23.10094f,-86.8612f, 32.41f, -85.222f)

    def textcondition[V](z:Entry[V]):Boolean=
    {
         z.value match
         {
           case v:String =>
             val vl=v.toLowerCase()
             (vl.contains("bitches")||vl.contains("bitch")||v.contains("bit"))
         }
    }

    val rangesearchresult=indexed.rangeFilter(searchbox,textcondition)

    println(rangesearchresult.size)

    rangesearchresult.foreach(println)

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

    val queryrdd=locationRDD.sample(false,0.01)

    val queryboxes=queryrdd.map{
      case (p:Point,v)=>
        val r=qtreeUtil.getRandomUniformPoint(5,5)
        (Box(p.x,p.y,p.x+r.x,p.y+r.y))
    }

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

    spark.stop()

  }
}
