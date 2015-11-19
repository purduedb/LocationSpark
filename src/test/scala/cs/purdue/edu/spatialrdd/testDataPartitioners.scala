package cs.purdue.edu.spatialrdd

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialrdd.impl._
import cs.purdue.edu.spatialindex.rtree._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by merlin on 9/23/15.
 */


object testDataPartitioners {

  def testGrid(): Unit =
  {
    val gridpartitioner=new Grid2DPartitioner(qtreeUtil.rangx,qtreeUtil.rangy,4)

    val point=Point(30.40094f,-86.8612f)

    val pid=gridpartitioner.getPartition(point)

    println(pid)
  }

  def test1(): Unit =
  {
    val box =Box(30.10094f,-86.8612f, 32.41f, -80.222f)

    val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangy,4)

    boxpartitioner.getPartitionsForBox(box).foreach(println)

    boxpartitioner.getPartitionsForRangeQuery(box).foreach(println)
  }


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("Test for Spark SpatialRDD").setMaster("local[2]")

    //val conf = new SparkConf().setAppName("Test for Spark SpatialRDD")

    val spark = new SparkContext(conf)

    require(args.length==1)

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

    val qtreepartioner=new QtreePartitioner(locationRDD.partitions.length,0.05f,locationRDD)

    val point=Point(30.40094f,-86.8612f)

    val pid=qtreepartioner.getPartition(point)
    println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    println(pid)

    val indexed = locationRDD.partitionBy(qtreepartioner)

    val box=Box(29.10094f,-87.8612f, 31.41f, -85.222f)
    println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    qtreepartioner.quadtree.getPIDforBox(box).foreach(println)



    /*def sumfunction[V](iterator: Iterator[V])=
    {
      println(iterator.size)
    }

    println("data in each partition")
    indexed.foreachPartition(sumfunction)*/



  }
}
