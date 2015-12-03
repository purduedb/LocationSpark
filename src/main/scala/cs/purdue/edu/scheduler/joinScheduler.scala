package cs.purdue.edu.scheduler

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree.Box
import cs.purdue.edu.spatialrdd.{SpatialRDDPartition, SpatialRDD}
import cs.purdue.edu.spatialrdd.impl.{Grid2DPartitioner, QtreePartitioner, Grid2DPartitionerForBox}
import org.apache.spark.rdd.{RDD}
import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

/**
 * Created by merlin on 11/30/15.
 * this class is used to handle the query skew
 */
class joinScheduler[K:ClassTag,V:ClassTag,U:ClassTag,T:ClassTag](datardd:SpatialRDD[K,V], queryrdd:RDD[U]) {


  private def getPartitionSize[T](rdd: RDD[T]): (Array[(Int, Int)]) = {
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      Iterator((idx, iter.size))
    }.collect()
    sketched
  }

  /**
   * get the statistic information for the input rdd
   */
  private def analysis(data:SpatialRDD[K,V], query:RDD[(K,U)]):IndexedSeq[(Int,Int,Int)]=
  {

    val stat_datardd=getPartitionSize(data).sortBy(_._1)
    val stat_queryrdd=getPartitionSize(query).sortBy(_._1)

    stat_datardd.map{
      case(id,size)=>
      stat_queryrdd.find(_._1==id).getOrElse(None)
        match{
          case (qid:Int,qsize:Int)=> (id,size,qsize)
          case None=>(id,size,0)
        }
    }.toIndexedSeq
  }

  /**
   * transform the query box rdd to key value pairs
   * @return
   */
  private def transformQueryRDD(): RDD[(K, U)] =
  {
    /**
     * map the rdd(box) to rdd(point, box)
     */
     def tranformRDDGridPartition[K: ClassTag, U: ClassTag](boxRDD: RDD[U], numpartition: Int): RDD[(K, U)] = {

    val boxpartitioner = new Grid2DPartitionerForBox(qtreeUtil.rangx, qtreeUtil.rangx, numpartition)

    boxRDD.flatMap {
      case (box: Box) =>
        boxpartitioner.getPartitionsForRangeQuery(box).map(p => (p.asInstanceOf[K], box.asInstanceOf[U]))
    }
  }

    /**
     * map the rdd(box) to rdd(point, box)
     */
     def tranformRDDQuadtreePartition[K: ClassTag, U: ClassTag](boxRDD: RDD[U], partionner: Option[org.apache.spark.Partitioner]):
    RDD[(K, U)] = {
    boxRDD.flatMap {
      case (box: Box) => {
        partionner.getOrElse(None) match {
          case qtreepartition: QtreePartitioner[K, V] =>
            qtreepartition.getPointsForSJoin(box).map(p => (p.asInstanceOf[K], box.asInstanceOf[U]))
        }
      }
    }
    }

    this.datardd.partitioner.getOrElse(None) match {

      case qtree: QtreePartitioner[K, V] =>

        tranformRDDQuadtreePartition[K, U](this.queryrdd, this.datardd.partitioner).partitionBy(this.datardd.partitioner.get)

      case grid: Grid2DPartitioner =>

        tranformRDDGridPartition[K, U](this.queryrdd, this.datardd.partitions.length).partitionBy(this.datardd.partitioner.get)

    }

  }

  /**
   * the algorithm is used to find the skewpartition based on the
   * pid, datasize, querysize
   * 1, 299,233
   * @param stat
   * @return
   */
  private def findSkewPartition(stat:IndexedSeq[(Int,Int,Int)]):Map[Int,Int]=
  {

    /**
     * option1: get the topk partitions based on the query size
     */
    val threshold=0.2
    val topk=(stat.size*threshold).toInt
    stat.sortWith(_._3>_._3).slice(0,topk).map(elem=>(elem._1,elem._2)).toMap

    /**
     * option2: herusitic and cost based partitions.
     * step1: get the half partition, run and get the execution time
     * step2:
     * step3:
     */

  }

  /**
   * this scheduler work as following
   * (1) get the statistic information for each partition
   * (2) partition the query and data rdd, based on statistic information
   * (3) execute the join for the newly generated rdd
   * @return RDD
   */
  def scheduleJoin():RDD[(K,V)]={

    val transformQueryrdd=transformQueryRDD()

    val stat=analysis(this.datardd,transformQueryrdd)

    //the core part for this scheduler
    val topKpartitions=findSkewPartition(stat)

    val broadcastVar = this.datardd.context.broadcast(topKpartitions)

    //transform the skew and query rdd
    val skew_queryrdd = transformQueryrdd.mapPartitionsWithIndex{
      (pid,iter)=>
        broadcastVar.value.contains(pid) match
        {
          case true=> iter
          case false=> Iterator.empty
        }
    }

    val skew_datardd= this.datardd.mapPartitionsWithIndex(
      (pid,iter)=> broadcastVar.value.contains(pid) match
      {
        case true=>iter
        case false=>Iterator.empty
      },true
    )

    val nonskew_queryrdd = transformQueryrdd.mapPartitionsWithIndex{
      (pid,iter)=>
        broadcastVar.value.contains(pid) match
        {
          case false=> iter
          case true=> Iterator.empty
        }
    }

    //the simplist way to implement here
    val nonskew_datardd =this.datardd

    /***************************************************************/
    /***********************execute join****************************/

    val skewindexrdd=SpatialRDD(skew_datardd)
    val part1=skewindexrdd.sjoins[U](skew_queryrdd)((k, id) => id)

    val part2=nonskew_datardd.sjoins(nonskew_queryrdd)((k, id) => id)
    /***************************************************************/

    part1.union(part2)

    //Array(skew_queryrdd,skew_datardd,nonskew_queryrdd,nonskew_datardd)
  }

}

/*
    println("*"*100)
    println(part1.count())

    println("*"*100)
    println(part2.count())

        //println("*"*100)
    //println(joinresult.count())
 */

//println("query summary")
//getPartitionSize(transformQueryrdd).sortBy(_._1).foreach(println)
//transformQueryrdd.foreach(println)

//println("X"*100)
//topKpartitions.foreach(println)

//println("non-skew query summary")
//getPartitionSize(nonskew_queryrdd).sortBy(_._1).foreach(println)

//println("spatial skew data rdd")
//getPartitionSize(skew_datardd).sortBy(_._1).foreach(println)
//println(skew_datardd.count())

/*new SpatialRDD(
  this.datardd.mapPartitionsWithIndex[SpatialRDDPartition[K, V]]{
    (pid,iter)=> broadcastVar.value.contains(pid) match
    {
      case false=>iter.asInstanceOf[Iterator[SpatialRDDPartition[K, V]]]
      case true=>Iterator.empty
    }
  }
)*/

/*
def nonskew_join():SpatialRDD[K,V]={
      nonskew_datardd match {
        case s: SpatialRDD[K, V] =>
          nonskew_queryrdd match {
            case q: RDD[_] =>
               s.sjoins[U](q.asInstanceOf[RDD[(K, U)]])((k, id) => id)
              //println(joinresult.count())
          }
      }
    }
  */

/*def skew_join():SpatialRDD[K,V]={
  skew_queryrdd match {
    case q: RDD[_]=>
      tmp1 match
      {
        case d:RDD[(K,V)]=>
          val indexrdd=SpatialRDD(d)
          indexrdd.sjoins[U](q.asInstanceOf[RDD[(K, U)]])((k, id) => id)
      }
  }
}*/