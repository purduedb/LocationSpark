package cs.purdue.edu.spatialrdd

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import cs.purdue.edu.spatialrdd.impl.{Grid2DPartitionerForBox, Grid2DPartitioner}
import org.apache.spark.{Partition, TaskContext, SparkContext, SparkConf}

/**
 * Created by merlin on 11/17/15.
 */
object NaiveRDD {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Test for Spark SpatialRDD").setMaster("local[2]")

    //val conf = new SparkConf().setAppName("Test for Spark SpatialRDD")

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


    val indexed = locationRDD.partitionBy(new Grid2DPartitioner(qtreeUtil.rangx, qtreeUtil.rangy, locationRDD.partitions.size))

    //indexed.partitions.foreach{case(p:Partition)=>}

    def sumfunction[V](iterator: Iterator[V])=
    {
      println(iterator.size)
    }

    println("data in each partition")
    indexed.foreachPartition(sumfunction)

    val box=Box(20.10094f,-86.8612f,32.41f, -80.222f)

    val rangeresult=indexed.filter{
      case((p:Point,value))=>
        box.contains(p)
      case _=>true
    }

    println("*"*10)
    println(rangeresult.count())
    //indexed.filterByRange(Point(20.10094f,-86.8612f), Point(32.41f, -80.222f))

    spark.stop()

  }

}
