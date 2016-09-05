package cs.purdue.edu.spatialrdd.impl

import cs.purdue.edu.scheduler.skewAnalysis
import cs.purdue.edu.spatialindex.quatree.QtreeForPartion
import cs.purdue.edu.spatialindex.rtree.{Box, Point, Geom}
import cs.purdue.edu.spatialrdd.SpatialRDD
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by merlin on 12/20/15.
 */
/**
 * because knn join is more complex than range join,
 * this class is specifically design for knn join function for spatial rdd
 */

/**
 * @param datardd spatialrdd
 * @param queryrdd points
 */
class knnJoinRDD[K:ClassTag,V:ClassTag]
  ( datardd:SpatialRDD[K,V],
    queryrdd:RDD[(K)],
    knn:Int,
    f1:(K)=>Boolean,
    f2:(V)=>Boolean
    ) extends Serializable
{

  private def getPartitionSize[T](rdd: RDD[T]): (Array[(Int, Int)]) = {
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      Iterator((idx, iter.size))
    }.collect()
    sketched
  }


  /**
   *this is the rangebased knn join
   * @return
   */
  def rangebasedKnnjoin():RDD[(K, Iterator[(K,V)])]=
  {

    val knn=this.knn
    //step1: partition the queryrdd if the partitioner of query and data rdd is different
    val tmpqueryrdd=queryrdd.map(key=>(key,knn))

    val partitionedRDD =tmpqueryrdd.partitionBy(datardd.partitioner.get)

    //localKnnJoinRDD with this format: RDD[p, iterator[(k,v)]]
    val localKnnJoinRDD = datardd.partitionsRDD.zipPartitions(partitionedRDD, true) {
          (thisIter, otherIter) =>
            val thisPart = thisIter.next()
           thisPart.knnjoin_(otherIter, knn, f1,f2)
        }.cache()

    //localKnnJoinRDD.collect().foreach(println)

    def distancetoBox(point:K,max:Double):Box=
    {
      val p=point.asInstanceOf[Point]
      Box((p.x-max).toFloat,(p.y-max).toFloat,(p.x+max).toFloat,(p.y+max).toFloat)
    }

    //step3: map the knn join to range join. execute the rjoin and return [q. iterator[(k,v)]]
    val pointboxrdd=localKnnJoinRDD.map{
      case(point, max,itr)=>
        (point,itr, distancetoBox(point,max))
    }


    val firstRoundKJOINwithBox=pointboxrdd.map
    {
      case(point,itr, box)=>
        this.datardd.partitioner.get match
        {
          case  qtreepartition:QtreePartitioner[K, V] =>
            (point, itr, qtreepartition.getPointsForSJoin(box),box)
        }
    }

    val correctKNN=firstRoundKJOINwithBox.filter{
      case(k,itr, hashset,box)=>
        hashset.size==1
    }.map
    {
      case(k,itr, hashset,box)=>
        (k,itr)
    }

    val nextRoundKNN=firstRoundKJOINwithBox.filter{
      case(k,itr,hashset,box)=>
        hashset.size>1
    }



    //option1: range search over overlap partitions
    def rangejoin():RDD[(K,Iterator[(K,V)])]=
    {
      //map this knnrdd to related partition rdd
      val pointstoPIDRDD=nextRoundKNN.flatMap {
        case(k,itr,hashset,box)=>
          hashset.map(p => (p.asInstanceOf[K], (k,itr,box)))
      }

      /*//this is the naive way to do this
      val partitioned = pointstoPIDRDD.partitionBy(this.datardd.partitioner.get)
      val newPartitionsRDD = this.datardd.partitionsRDD.zipPartitions(partitioned, true)
          {
            (thisIter, otherIter) =>
              val thisPart = thisIter.next()
              //execute the rangebased knn join
              thisPart.rkjoin(otherIter,f1,f2,knn)
          }*/

      /******************************************************************************/
      /******************************add scheduler to this part**********************/

      val stat=analysis(this.datardd,pointstoPIDRDD)

      val topKpartitions=skewAnalysis.findTopKSkewPartition(stat,5)

      val broadcastVar = this.datardd.context.broadcast(topKpartitions)

      //transform the skew and query rdd
      val skew_queryrdd = pointstoPIDRDD.mapPartitionsWithIndex{
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

      val nonskew_queryrdd = pointstoPIDRDD.mapPartitionsWithIndex{
        (pid,iter)=>
          broadcastVar.value.contains(pid) match
          {
            case false=> iter
            case true=> Iterator.empty
          }
      }

      val nonskew_datardd =this.datardd

      def getPartitionerbasedQuery(topKpartitions:Map[Int,Int], skewQuery:RDD[(K,(K,Iterator[(K,V)],Box))]):
      QtreePartitionerBasedQueries[Int,QtreeForPartion] =
      {
        //default nubmer of queries
        val samplequeries=skewQuery.sample(false,0.02f).map{case(point, (pt,itr,box))=>box}.distinct().collect()

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

        val partitionnumberfromQueries= qtreepartition.computePIDBasedQueries(topKpartitions)

        new QtreePartitionerBasedQueries(partitionnumberfromQueries,qtreepartition)

      }

      val newpartitioner=getPartitionerbasedQuery(topKpartitions,skew_queryrdd)
      val skewindexrdd=SpatialRDD.buildSRDDwithgivenPartitioner(skew_datardd,newpartitioner)

      //execute the join between the skew index rdd with skew_queryrdd
      val repartitionSkewQueryRDD = skew_queryrdd.partitionBy(newpartitioner)
      val skewresultRDD = skewindexrdd.partitionsRDD.zipPartitions(repartitionSkewQueryRDD, true)
      {
        (thisIter, otherIter) =>
          val thisPart = thisIter.next()
          thisPart.rkjoin(otherIter,f1,f2,knn)
      }

      //execute the join between the non skew index rdd with non skew_queryrdd
      val nonskewresultRDD = nonskew_datardd.partitionsRDD.zipPartitions(nonskew_queryrdd, true)
      {
        (thisIter, otherIter) =>
          val thisPart = thisIter.next()
          thisPart.rkjoin(otherIter,f1,f2,knn)
      }



      /******************************************************************************/
      /***************************finish the skew part ******************************/
      /******************************************************************************/

      val joinResultRDD=skewresultRDD.union(nonskewresultRDD)

      /******************************************************************************/


      joinResultRDD.reduceByKey((itr1,itr2)=>itr1++itr2,correctKNN.partitions.size/2).map
      {
        case(querypoint,itr)=>

          val tmpit=itr.map
          {
            case(location,value)=>
              (querypoint.asInstanceOf[Geom].distance(location.asInstanceOf[Point]),location,value)
          }.toArray.sortBy(_._1).distinct.slice(0,knn).map
          {
            case(distance,p,value)=>(p,value)
          }.toIterator

          (querypoint,tmpit)
      }

      //newPartitionsRDD.reduceByKey((itr1,itr2)=>itr1++itr2,correctKNN.partitions.size/2)
    }

    val rangejoinforknnRDD=rangejoin()

      rangejoinforknnRDD.union(correctKNN)
    //correctKNN
    //correctKNN.union(rangejoinforknnRDD)
    //correctKNN
  }


  /**
   * todo: add the herusitic way to search for the range knn join
   */
  def herusticknnjoin()={

  }

  /**
   * get the statistic information for the input rdd
   */
  private def analysis(data:SpatialRDD[K,V], query:RDD[(K,(K,Iterator[(K,V)],Box))]):IndexedSeq[(Int,Int,Int)]=
  {

    val stat_datardd=data.mapPartitionsWithIndex { (idx, iter) =>
      Iterator((idx, iter.size))
    }.collect()

    val stat_queryrdd=query.mapPartitionsWithIndex { (idx, iter) =>
      Iterator((idx, iter.size))
    }.collect()

    stat_datardd.map{
      case(id,size)=>
        stat_queryrdd.find(_._1==id).getOrElse(None)
        match{
          case (qid:Int,qsize:Int)=> (id,size,qsize)
          case None=>(id,size,0)
        }
    }.toIndexedSeq
  }

}



/* val furtherrefineRDD=pointboxrdd.filter
 {
   case(point,box)=>
     datardd.partitioner.get match
     {
       case  qtreepartition:QtreePartitioner[K, V] =>
        qtreepartition.getPartitionForBox(box).size>1
     }
 }*/
//println("further size "+furtherrefineRDD.count())
//println("original size "+ localKnnJoinRDD.count())

//val stat_datardd=getPartitionSize(this.datardd).sortBy(_._1)
//println("data distribution")
//stat_datardd.foreach(println)


//println("query distribution")
//getPartitionSize(partitionedRDD).sortBy(_._1).foreach(println)
