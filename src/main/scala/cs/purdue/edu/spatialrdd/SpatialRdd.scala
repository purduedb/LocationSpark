package cs.purdue.edu.spatialrdd

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{TaskContext, Partition, OneToOneDependency}
import org.apache.spark.rdd.RDD

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

  /*************************************************/



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
}
