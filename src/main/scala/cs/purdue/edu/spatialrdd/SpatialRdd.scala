package cs.purdue.edu.spatialrdd

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree._
import cs.purdue.edu.spatialrdd.impl._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{TaskContext, Partition, OneToOneDependency}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by merlin on 8/4/15.
 */

class SpatialRDD[K: ClassTag, V: ClassTag]
  (
    private val partitionsRDD: RDD[SpatialRDDPartition[K, V]]
  )
  extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
{

  require(partitionsRDD.partitioner.isDefined)

  override val partitioner = partitionsRDD.partitioner


  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  override def setName(_name: String): this.type = {
    partitionsRDD.setName(_name)
    this
  }

  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /** Provides the `RDD[(K, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    firstParent[SpatialRDDPartition[K, V]].iterator(part, context).next.iterator
  }

  /*************************************************/
  /**********put k,v pair into the exist spatialRDD*/
  /**
   * Unconditionally updates the specified key to have the specified value. Returns a new IndexedRDD
   * that reflects the modification.
   */
  def put(k: K, v: V): SpatialRDD[K, V] = multiput(Map(k -> v))

  /**
   * Unconditionally updates the keys in `kvs` to their corresponding values. Returns a new
   * IndexedRDD that reflects the modification.
   */
  def multiput(kvs: Map[K, V]): SpatialRDD[K, V] = multiput[V](kvs, (id, a) => a, (id, a, b) => b)

  /**
   * Updates the keys in `kvs` to their corresponding values, running `merge` on old and new values
   * if necessary. Returns a new IndexedRDD that reflects the modification.
   */
  def multiput(kvs: Map[K, V], merge: (K, V, V) => V): SpatialRDD[K, V] =
    multiput[V](kvs, (id, a) => a, merge)

  /**
   * Updates the keys in `kvs` to their corresponding values, running `merge` on old and new values
   * if necessary. Returns a new IndexedRDD that reflects the modification.
   */
  def multiput[U: ClassTag](kvs: Map[K, U], z: (K, U) => V, f: (K, V, U) => V): SpatialRDD[K, V] = {

    val updates = context.parallelize(kvs.toSeq).partitionBy(partitioner.get)

    zipPartitionsWithOther(updates)(new MultiputZipper(z, f))

  }

  /** Gets the value corresponding to the specified key, if any. */
  def get(k: K): Option[V] = multiget(Array(k)).get(k)

  /** Gets the values corresponding to the specified keys, if any. */
  def multiget(ks: Array[K]): Map[K, V] = {

    val ksByPartition = ks.groupBy(k => partitioner.get.getPartition(k))
    val partitions = ksByPartition.keys.toSeq

    val results: Array[Array[(K, V)]] = context.runJob(partitionsRDD,
      (context: TaskContext, partIter: Iterator[SpatialRDDPartition[K, V]]) => {
        if (partIter.hasNext && ksByPartition.contains(context.partitionId)) {
          val part = partIter.next()
          val ksForPartition = ksByPartition.get(context.partitionId).get
          part.multiget(ksForPartition.iterator).toArray
        } else {
          Array.empty
        }
      }, partitions, allowLocal = true)

    results.flatten.toMap
  }

  /*************************************************/

  /** Gets the values corresponding to the specific box, if any. */
  def rangeFilter[U](box:U,z:Entry[V]=>Boolean): Map[K, V] = {

    //val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangy,this.partitions.size)

    //val ksByPartition = ks.map(k => boxpartitioner.getPartitions(k))
    //val partitionset = boxpartitioner.getPartitionsForBox(box)

    var partitionset=new mutable.HashSet[Int]

    this.partitioner.getOrElse(None) match{

      case qtree:QtreePartitioner[K,V]=>
        partitionset=qtree.getPartitionForBox(box)

      case grid:Grid2DPartitioner=>
        val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangy,this.partitions.size)
        partitionset = boxpartitioner.getPartitionsForBox(box)

      case None=>
        return Map.empty
    }

    //println("get the location of partition")
    //partitionset.foreach(println)

    val results: Array[Array[(K, V)]] = context.runJob(partitionsRDD,
      (context: TaskContext, partIter: Iterator[SpatialRDDPartition[K, V]]) => {
        if (partIter.hasNext && partitionset.contains(context.partitionId)) {
          val part = partIter.next()
          part.filter[U](box,z).toArray
        } else {
          Array.empty
        }
      }, partitionset.toSeq, allowLocal = true)

    results.flatten.toMap

  }

  /** Gets k-nearset-neighbor values corresponding to the specific point, if any. */
  def knnFilter[U](entry:U, k:Int, z:Entry[V]=>Boolean): Iterator[(K, V)] = {

    val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangy,this.partitions.size)

    //val ksByPartition = ks.map(k => boxpartitioner.getPartitions(k))
    val partitionid = boxpartitioner.getPartition(entry)

    /**
     * find the knn point in certain partition
     */
    val results: Array[Array[(K, V,Double)]] = context.runJob(partitionsRDD,
      (context: TaskContext, partIter: Iterator[SpatialRDDPartition[K, V]]) => {
        if (partIter.hasNext && partitionid==context.partitionId) {
          val part = partIter.next()
          part.knnfilter[U](entry,k,z).toArray
        } else {
          Array.empty
        }
      }, Seq(partitionid), allowLocal = true)


    /**
     * draw the circle, and do the range search over those overlaped partitions
     */
    val (_,_,distance)=results.flatten.tail.toSeq.head


    /**
     *get the box around the center point
     */
    def getbox(entry:U, range:Double):Box=
    {
      entry match
      {
        case point:Point=>
          val trange=range.toFloat
          Box(point.x-trange,point.y-trange,point.x+trange,point.y+trange)
      }
    }

    val rangequery=this.rangeFilter(getbox(entry,distance),z)

    //println("range query result")
    //rangequery.foreach(println)
    /**
     * merge the range query and knn query results
     */
    val rangequerieswithdistance=rangequery.map{
      case(location:Point,value)=> (location,value, entry.asInstanceOf[Point].distanceSquared(location))
    }.toList

    //var pids=new HashSet[(Point,Double)]

    val knnresultwithdistance=results.flatten.map{
      case(location:Point,value,distance)=>
        (location,value, distance)
    }.toList

    val finalresult=(knnresultwithdistance++rangequerieswithdistance).sortBy(_._3).distinct.slice(0,k)

    //finalresult.foreach(println)

    finalresult.map{
      case(location:Point,value,distance) =>(location.asInstanceOf[K],value)
    }.toIterator


  }

  /**
   * Deletes the specified keys. Returns a new spatialRDD that reflects the deletions.
   */
  def delete(ks: Array[K]): SpatialRDD[K, V] = {
    val deletions = context.parallelize(ks.map(k => (k, ()))).partitionBy(partitioner.get)
    zipPartitionsWithOther(deletions)(new DeleteZipper)
  }


  /**
   * this rdd would be the data rdd, and other rdd is the spatial range query rdd
   * this data rdd key is the location of data, value the correspond data
   * this spatial range query rdd, with key is the location of the query box, the value is the range query box
   * Notice:
   * (1)the rdd.sjoin(other)!=other.sjoin(rdd)
   * (2)the other rdd have key and box paris
   */
  def sjoins[U: ClassTag]
  (other: RDD[(K, U)])(f: (K, V) => V): SpatialRDD[K, V] =
    other match {
    case other: SpatialRDD[K, U] if partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new JoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherJoinZipper(f))
  }

  /**
   * this rdd would be the data rdd, and other rdd is the spatial range query rdd
   * this data rdd key is the location of data, value the correspond data
   * this spatial range query rdd, with key is the location of the query box, the value is the range query box
   * Notice:
   * (1)the rdd.sjoin(other)!=other.sjoin(rdd)
   * (2)the other rdd(box)
   */
  def sjoin[U: ClassTag]
  (other: RDD[U])(f: (K, V) => V): SpatialRDD[K, V] = {

    /**
     * map the rdd(box) to rdd(point, box)
     */
    def tranformRDDGridPartition[K: ClassTag, U: ClassTag](queriesboxes: RDD[U], numpartition: Int): RDD[(K, U)] = {

      val boxpartitioner = new Grid2DPartitionerForBox(qtreeUtil.rangx, qtreeUtil.rangx, numpartition)

      queriesboxes.flatMap {
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

    //transform this rdd(box) into a RDD(point, box)
    this.partitioner.getOrElse(None) match {

      case qtree: QtreePartitioner[K, V] =>

        val queriesRDD = tranformRDDQuadtreePartition[K, U](other, this.partitioner)

        queriesRDD match {
          case other: SpatialRDD[K, U] if partitioner == other.partitioner =>
            this.zipIndexedRDDPartitions(other)(new JoinZipper(f))

          case other: RDD[(K, U)] =>
            this.zipPartitionsWithOther(other)(new OtherJoinZipper(f))
        }

      case grid: Grid2DPartitioner =>

        val queriesRDD = tranformRDDGridPartition[K, U](other, this.partitions.length)

        queriesRDD match {
          case other: SpatialRDD[K, U] if partitioner == other.partitioner =>
            this.zipIndexedRDDPartitions(other)(new JoinZipper(f))

          case other: RDD[(K, U)] =>
            this.zipPartitionsWithOther(other)(new OtherJoinZipper(f))
        }

      case None => this

    }

  }


  /** Applies a function to corresponding partitions of `this` and another IndexedRDD. */
  private def zipIndexedRDDPartitions[V2: ClassTag, V3: ClassTag]
  (other: SpatialRDD[K, V2]) (f: ZipPartitionsFunction[V2, V3]): SpatialRDD[K, V3] = {
    assert(partitioner == other.partitioner)
    val newPartitionsRDD = partitionsRDD.zipPartitions(other.partitionsRDD, true)(f)
    new SpatialRDD(newPartitionsRDD)
  }

  /*************************************************/

  /** Applies a function to corresponding partitions of `this` and a pair RDD. */
  private def zipPartitionsWithOther[V2: ClassTag, V3: ClassTag]
      (other: RDD[(K, V2)])
      (f: OtherZipPartitionsFunction[V2, V3]):
      SpatialRDD[K, V3] = {
    val partitioned = other.partitionBy(partitioner.get)
    val newPartitionsRDD = partitionsRDD.zipPartitions(partitioned, true)(f)
    new SpatialRDD(newPartitionsRDD)
  }

  // The following functions could have been anonymous, but we name them to work around a Scala
  // compiler bug related to specialization.

  private type ZipPartitionsFunction[V2, V3] =
  Function2[Iterator[SpatialRDDPartition[K, V]], Iterator[SpatialRDDPartition[K, V2]],
    Iterator[SpatialRDDPartition[K, V3]]]

  private type OtherZipPartitionsFunction[V2, V3] =
  Function2[Iterator[SpatialRDDPartition[K, V]], Iterator[(K, V2)],
    Iterator[SpatialRDDPartition[K, V3]]]

  private class MultiputZipper[U](z: (K, U) => V, f: (K, V, U) => V)
    extends OtherZipPartitionsFunction[U, V] with Serializable {
    def apply(thisIter: Iterator[SpatialRDDPartition[K, V]], otherIter: Iterator[(K, U)])
    : Iterator[SpatialRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.multiput(otherIter, z, f))
    }
  }

  private class DeleteZipper extends OtherZipPartitionsFunction[Unit, V] with Serializable {
    def apply(thisIter: Iterator[SpatialRDDPartition[K, V]], otherIter: Iterator[(K, Unit)])
    : Iterator[SpatialRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.delete(otherIter.map(_._1.asInstanceOf[Entry[V]])))
    }
  }

  private class JoinZipper[U: ClassTag](f: (K, V) => V)
    extends ZipPartitionsFunction[U, V] with Serializable {
    def apply(thisIter: Iterator[SpatialRDDPartition[K, V]], otherIter: Iterator[SpatialRDDPartition[K, U]]):
    Iterator[SpatialRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.sjoin(otherPart)(f))
    }
  }

  private class OtherJoinZipper[U: ClassTag](f: (K, V) => V)
    extends OtherZipPartitionsFunction[U, V] with Serializable {
    def apply(thisIter: Iterator[SpatialRDDPartition[K, V]], otherIter: Iterator[(K, U)]):
    Iterator[SpatialRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.sjoin(otherIter)(f))
    }
  }
}

object SpatialRDD {
  /**
   * Constructs an updatable IndexedRDD from an RDD of pairs, merging duplicate keys arbitrarily.
   */
  def apply[K: ClassTag, V: ClassTag]
  (elems: RDD[(K, V)]): SpatialRDD[K, V] = updatable(elems)

  /**
   * Constructs an updatable IndexedRDD from an RDD of pairs, merging duplicate keys arbitrarily.
   */
  def updatable[K: ClassTag , V: ClassTag]
  (elems: RDD[(K, V)])
  : SpatialRDD[K, V] = updatable[K, V, V](elems, (id, a) => a, (id, a, b) => b)

  /** Constructs an IndexedRDD from an RDD of pairs.
    * the default partitioner is the quadtree based partioner
    * */
  def updatable[K: ClassTag , U: ClassTag, V: ClassTag]
  (elems: RDD[(K, V)], z: (K, U) => V, f: (K, V, U) => V)
  : SpatialRDD[K, V] = {
    val elemsPartitioned =
        //elems.partitionBy(new Grid2DPartitioner(qtreeUtil.rangx, qtreeUtil.rangy, elems.partitions.size))
      elems.partitionBy(new QtreePartitioner(elems.partitions.length,0.001f,elems))

    val partitions = elemsPartitioned.mapPartitions[SpatialRDDPartition[K, V]](
      iter => Iterator(RtreePartition(iter, z, f)),
      preservesPartitioning = true)
    new SpatialRDD(partitions)
  }

}
