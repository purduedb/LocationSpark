package cs.purdue.edu.spatialrdd.main

import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import cs.purdue.edu.spatialindex.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialrdd.impl.QtreePartitioner
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by merlin on 11/17/15.
 */
object NaiveRDD {

  def main(args: Array[String]) {

    //val conf = new SparkConf().setAppName("Test for Spark SpatialRDD").setMaster("local[2]")

   val conf = new SparkConf().setAppName("Test for Spark SpatialRDD")

    val spark = new SparkContext(conf)

    require(args.length==2)

    val inputfile=args(0)
    val sample=args(1).toFloat

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


    val quadtreePartitioner=new QtreePartitioner(500,0.01f,locationRDD)

    val indexed = locationRDD.map{
      case(point,v)=>
        (quadtreePartitioner.getPartition(point),(point,v))
    }

    val queryrdd=locationRDD.sample(false,sample)


    val queryboxes=queryrdd.map{
      case (p:Point,v)=>
        val r=qtreeUtil.getRandomUniformPoint(3,3)
        (Box(p.x,p.y,p.x+r.x,p.y+r.y))
    }

    val queryboxRDD=queryboxes.flatMap {
      case (box: Box) => {
        quadtreePartitioner.quadtree.getPIDforBox(box).map(pid => (pid, box))
      }
    }

    val sjoinresult2=indexed.join(queryboxRDD).filter
    {
      case(partitionid,((po:Point,value),b:Box))=>
        b.contains(po)
    }.map
    {
      case(partitionid,((po:Point,value),b:Box))=>
        (b,(po,value))
    }.aggregateByKey(0,indexed.partitions.length/2) (
      (k,v) => 1, (v,k) => k+v
    )

    println(sjoinresult2.count)


    spark.stop()

  }

}

/*val sjoinresult=indexed.join(queryboxRDD).filter
{
  case(partitionid,((po:Point,value),b:Box))=>
    b.contains(po)
}.map
{
  case(partitionid,((po:Point,value),b:Box))=>
    (b,(po,value))
}.groupByKey(indexed.partitions.length/2).
  map{
  case(b,itr:Iterable[(Point,Any)])=>(b,itr.size)
}.reduceByKey(_+_)*/