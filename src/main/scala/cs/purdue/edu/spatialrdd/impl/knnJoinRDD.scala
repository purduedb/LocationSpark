package cs.purdue.edu.spatialrdd.impl

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

  def rangebasedKnnjoin():RDD[(K, Iterator[(K,V)])]=
  {

    //val stat_datardd=getPartitionSize(this.datardd).sortBy(_._1)
    //println("data distribution")
    //stat_datardd.foreach(println)

    val knn=this.knn
    //step1: partition the queryrdd if the partitioner of query and data rdd is different
    val tmpqueryrdd=queryrdd.map(key=>(key,knn))

    val partitionedRDD =tmpqueryrdd.partitionBy(datardd.partitioner.get)

    //println("query distribution")
    //getPartitionSize(partitionedRDD).sortBy(_._1).foreach(println)

    //localKnnJoinRDD with this format: RDD[p, iterator[(k,v)]]
    val localKnnJoinRDD = datardd.partitionsRDD.zipPartitions(partitionedRDD, true) {
          (thisIter, otherIter) =>
            val thisPart = thisIter.next()
           thisPart.knnjoin_(otherIter, f1,f2)
        }.cache()

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
            (point, itr, qtreepartition.getPointsForSJoin(box))
        }
    }

    val correctKNN=firstRoundKJOINwithBox.filter{
      case(k,itr, hashset)=>
        hashset.size==1
    }.map
    {
      case(k,itr, hashset)=>
        (k,itr)
    }

    val nextRoundKNN=firstRoundKJOINwithBox.filter{
      case(k,itr,hashset)=>
        hashset.size>1
    }

    //option1: range search over whole overlap partitions
    def rangejoin():RDD[(K,Iterator[(K,V)])]=
    {
      //map this knnrdd to related partition rdd
      val pointstoPIDRDD=nextRoundKNN.flatMap {
        case(k,itr,hashset)=>
          hashset.map(p => (p.asInstanceOf[K], (k,itr)))
      }

      val partitioned = pointstoPIDRDD.partitionBy(this.datardd.partitioner.get)

      val newPartitionsRDD = this.datardd.partitionsRDD.zipPartitions(partitioned, true)
          {
            (thisIter, otherIter) =>
              val thisPart = thisIter.next()
              //execute the rangebased knn join
              thisPart.rkjoin(otherIter,f1,f2)
          }


      newPartitionsRDD.reduceByKey((itr1,itr2)=>itr1++itr2,correctKNN.partitions.size/2).map
      {
        case(querypoint,itr)=>
          val tmpit=itr.map
          {
            case(location,value)=>
              (querypoint.asInstanceOf[Geom].distance(location.asInstanceOf[Point]),location,value)

          }.toList.sortBy(_._1).distinct.slice(0,knn).map
          {
            case(distance,p,value)=>(p,value)
          }.toIterator

          //this is used for fixing the seriaziable problem
          val tmparray=ArrayBuffer.empty[(K,V)]
          tmpit.foreach{case(k,v)=>tmparray.append((k,v))}

          (querypoint,tmparray.toIterator)
      }

    }

    val rangejoinforknnRDD=rangejoin()

    correctKNN.union(rangejoinforknnRDD)

  }


  def herusticknnjoin()={

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