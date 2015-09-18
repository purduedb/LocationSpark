package cs.purdue.edu.spatialrdd.impl

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.util.Utils


class GridSpatialPartitioner(partitionx: Int,partitiony: Int) extends Partitioner{

  def numPartitions: Int = partitionx*partitiony

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def getPartition(key: Any): Int = key match {

    case null => 0

    case _ => nonNegativeMod(key.hashCode, numPartitions)
  }

  override def hashCode: Int = numPartitions

}

/**
 *spatial venioam spatial partition
 */
/*class VGramSpatialPartitioner(partitionx: Int) extends Partitioner{

  def numPartitions: Int = partitionx

  def getPartition(key: Any): Int = key match {

    case null => 0

    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def hashCode: Int = numPartitions

}*/
