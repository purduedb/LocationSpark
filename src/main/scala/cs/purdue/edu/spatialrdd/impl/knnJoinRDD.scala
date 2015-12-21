package cs.purdue.edu.spatialrdd.impl

import cs.purdue.edu.spatialrdd.SpatialRDD
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by merlin on 12/20/15.
 */
/**
 * because knn join is more complex than range join,
 * this class is specifically design for knn join of spatial rdd
 */

/**
 * @param datardd spatialrdd
 * @param queryrdd points
 * @param k k-nn join
 */
class knnJoinRDD[K:ClassTag,V:ClassTag,U:ClassTag,T:ClassTag]
  (datardd:SpatialRDD[K,V], queryrdd:RDD[(K,U)], k:Int)
{

  /*
  def naiveknnjoin():RDD[(K, Iterator[(K,V)])]=
  {
    //step1: partition the queryrdd if the partitioner of query and data rdd is different
        val partitioned = this.queryrdd.partitionBy(datardd.partitioner.get)

    //localKnnJoinRDD with this format: RDD[p, iterator[(k,v)]]
       /*val localKnnJoinRDD = datardd.partitionsRDD.zipPartitions(partitioned, true) {
          (thisIter, otherIter) =>
            val thisPart = thisIter.next()
          //step2: local knn join, return the q, iterator[(k,v)]
           //thisPart.rjoin(otherIter)(f, f2)
        }*/

    //step3: map the knn join to range join. execute the rjoin and return [q. iterator[(k,v)]]


    //step4: merge the join results reduce(iterator[(k,v)], iterator[(k,v)]) to get [q,iterator[(k,v)]]

  }*/


  def adpativeknnjoin()={

  }

}
