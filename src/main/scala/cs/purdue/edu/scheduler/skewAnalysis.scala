package cs.purdue.edu.scheduler

import scala.collection.mutable

/**
 * Created by merlin on 12/9/15.
 */
class skewAnalysis {

  /**
   * find the skew partition, and find the partition approach for those skew partition
   * @param stat
   * @param maxPartition
   * @return
   */
  private def findSkewPartition(stat:IndexedSeq[(Int,Int,Int)], maxPartition:Int):Map[Int,Int]=
  {

    //find the partition with very few number of queries
    //for example, number of queries in that partition<100
    var available=stat.size

    val withoutempty=stat.filter(e=>e._3!=0)

    //val maxability=
    val n=withoutempty.size
    available=maxPartition+(available-withoutempty.size) //avlaible size

    //find those skew partitions,
    val sortlist=withoutempty.sortBy(r => (r._3*r._2))

    var prefixquery=(sortlist(0)._3)
    var prefixdata=(sortlist(0)._2)

    var optimal=1
    var index= -1
    var mindistance=Int.MaxValue
    var tmpoptimal=1

    var minindex=1

    val alfa=0.001

    for( i <- 1 to n-1)
    {
      available+=1

      prefixquery=prefixquery+(sortlist(i)._3)
      prefixdata=prefixdata+(sortlist(0)._2)

      val average=prefixquery/(available)

      //want the average reach the nonskew part,
      // it means that we can repartition those nonskew to get the best performance
      if(average<=sortlist(i+1)._3)
      {
          if(sortlist(i)._3-average<mindistance)
          {
            index=i
            optimal=average
            mindistance=sortlist(i+1)._3-average
          }
      }

      //move to next, can reach the optimal
      if(sortlist(i)._3-average<mindistance)
      {
        //if move to next, do not increase repartition overhead greatly
        if(mindistance-sortlist(i)._3-average>sortlist(i)._2*alfa)
        {
          mindistance=sortlist(i+1)._3-average
          minindex=i
          tmpoptimal=average
        }

      }

    }

    if(index<0)
    {
      index=minindex
      optimal=tmpoptimal
    }

    //if we find this point, we find the rule to partition the skewpart
    val map=mutable.HashMap.empty[Int,Int]

    for( i <- 1 to index)
    {
      val number=(sortlist(i)._3/optimal)

      map.+=(sortlist(i)._1->number)
    }

    map.toMap
  }


  /**
   * find the skew partition, and find the partition approach for those skew partition
   * @param stat
   * @param threshold
   * @return
   */
  private def findSkewPartition(stat:IndexedSeq[(Int,Int,Int)], threshold:Double):Map[Int,Int]=
  {

    //if we find this point, we find the rule to partition the skewpart
    val map=mutable.HashMap.empty[Int,Int]

    //find those skew partitions,
    val sortlist=stat.sortBy(r => (r._3*r._2))

    var topk=(stat.size*threshold).toInt
    var ratio=sortlist(0)._3/sortlist(topk)._3

    var tmplist=IndexedSeq.empty[(Int,Int,Int)]

    if(ratio>3) //find the correct point
    {
      tmplist=sortlist.slice(0,topk)

    }else
    {
      topk=sortlist.size/2
      ratio=sortlist(0)._3/sortlist(topk)._3
      tmplist=sortlist.slice(0,topk)
    }

    tmplist.foreach
    {
      element=>map.+=(element._1->(element._3/ratio))
    }

    map.toMap
  }



}
