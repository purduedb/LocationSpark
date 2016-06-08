package cs.purdue.edu.scheduler

import cs.purdue.edu.spatialindex.quatree.QtreeForPartion
import cs.purdue.edu.spatialindex.rtree.{Box}
import cs.purdue.edu.spatialindex.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialrdd.{SpatialRDD}
import cs.purdue.edu.spatialrdd.impl.{QtreePartitionerBasedQueries, Grid2DPartitioner, QtreePartitioner, Grid2DPartitionerForBox}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDD}
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
   * this scheduler work as following
   * (1) get the statistic information for each partition
   * (2) partition the query and data rdd, based on statistic information
   * (3) execute the join for the newly generated rdd
   * @return RDD
   */
  @DeveloperApi
  def scheduleJoin():RDD[(K,V)]={

    val transformQueryrdd=transformQueryRDD()

    val stat=analysis(this.datardd,transformQueryrdd)

    //the core part for this scheduler
    //val topKpartitions=skewAnalysis.findSkewPartition(stat,0.5)

    val topKpartitions=skewAnalysis.findSkewPartitionQuery(stat,0.5)
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
    /**
     * below the the option 1, build the spatialrdd for the nonskew, then do the join
     */
    //val skewindexrdd=SpatialRDD(skew_datardd)
    //val part1=skewindexrdd.sjoins[U](skew_queryrdd)((k, id) => id)

    /**
     * below the the option 2, get the new data partitioner based on the query, then do the join
     */
    val newpartitioner=getPartitionerbasedQuery(topKpartitions,skew_queryrdd)
    val skewindexrdd=SpatialRDD.buildSRDDwithgivenPartitioner(skew_datardd,newpartitioner)

    val part1=skewindexrdd.sjoins[U](skew_queryrdd)((k, id) => id)

    /*************************************************************/
    val part2=nonskew_datardd.sjoins(nonskew_queryrdd)((k, id) => id)
    /***************************************************************/
    // part2
     part1.union(part2)
    //Array(skew_queryrdd,skew_datardd,nonskew_queryrdd,nonskew_datardd)
  }

  /**
   * this scheduler work as following
   * (1) get the statistic information for each partition
   * (2) partition the query and data rdd, based on statistic information
   * (3) execute the join for the newly generated rdd
   * @return RDD
   */
  def scheduleRJoin[U2:ClassTag](f1:(Iterator[(K,V)]) => U2, f2:(U2,U2)=>U2):
  RDD[(U,U2)]={

    val transformQueryrdd=transformQueryRDD()

    val stat=analysis(this.datardd,transformQueryrdd)

    val topKpartitions=skewAnalysis.findSkewPartitionQuery(stat,0.5)
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
    /**
     * below the the option 2, get the new data partitioner based on the query, then do the join
     */
    val newpartitioner=getPartitionerbasedQuery(topKpartitions,skew_queryrdd)
    val skewindexrdd=SpatialRDD.buildSRDDwithgivenPartitioner(skew_datardd,newpartitioner)

    val tmp1=skewindexrdd.rjoins[U,U2](skew_queryrdd)(f1,f2)
    val part1=tmp1.reduceByKey(f2, tmp1.partitions.length/2)

    /*************************************************************/
    val tmp2=nonskew_datardd.rjoins[U,U2](nonskew_queryrdd)(f1,f2)
    val part2=tmp2.reduceByKey(f2,tmp2.partitions.length/2)
    /***************************************************************/
     val tmp3=part1.union(part2)
    tmp3.reduceByKey(f2,tmp3.partitions.length/2)
    //Array(skew_queryrdd,skew_datardd,nonskew_queryrdd,nonskew_datardd)
  }
  /**
   * get the partitioner based on the query distribution
   * @param topKpartitions
   */
  private def getPartitionerbasedQuery(topKpartitions:Map[Int,Int], skewQuery:RDD[(K,U)]): QtreePartitionerBasedQueries[Int,QtreeForPartion] =
  {
    //default nubmer of queries
    //al samplequeries=this.queryrdd.sample(false,0.05f).collect()
    val samplequeries=skewQuery.sample(false,0.02f).map{case(point, box)=>box}.distinct().collect()

    //get the quadtree partionner from this data rdd, and colone that quadtree
    val qtreepartition=new QtreeForPartion(100)
    this.datardd.partitioner.getOrElse(None) match {
      case qtreepter: QtreePartitioner[K, V] =>
        val newrootnode=qtreepter.quadtree.coloneTree()
        //qtreepter.quadtree.printTreeStructure()
        qtreepartition.root=newrootnode
    }

    //run those queries over the old data partitioner
    samplequeries.foreach
    {
      case box:Box=>qtreepartition.visitleafForBox(box)
    }

    //topKpartitions.foreach(println)

    //get the new partitionid based on those queries
    val partitionnumberfromQueries= qtreepartition.computePIDBasedQueries(topKpartitions)

    //qtreepartition.printTreeStructure()
    //println("partitionnumber"+partitionnumberfromQueries)

    new QtreePartitionerBasedQueries(partitionnumberfromQueries,qtreepartition)

  }

}

/*

    println("nonskew "+nonskew_queryrdd.count())
    println("skew "+skew_queryrdd.count())
    println("total "+transformQueryrdd.count())

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

/**
 * option2: do not build index, just use the rdd join function to execute the spatial join
 */
/* val quadtreePartitioner=new QtreePartitioner(skew_datardd.partitions.length,0.01f,skew_datardd)

 val indexed = skew_datardd.map{
   case(point,v)=>
     (quadtreePartitioner.getPartition(point),(point,v))
 }

 val queryboxRDD= skew_queryrdd.map{case(point, box)=>box}.distinct().flatMap {
   case (box) => {
     quadtreePartitioner.quadtree.getPIDforBox(box.asInstanceOf[Box]).map(pid => (pid, box))
   }
 }

 val part1=indexed.join(queryboxRDD).filter
 {
   case(pid,((po:Point,value),b:Box))=>
     b.contains(po)
 }.map
 {
   case(pid,((po,value),b:Box))=>
     (po,value)
 }*/

//val spart1=SpatialRDD(part1)