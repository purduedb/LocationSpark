package cs.purdue.edu.spatialrdd.impl

import org.apache.spark.Partitioner
import cs.purdue.edu.spatialindex.rtree._

/**
 * static data partition approach
 */
class Grid2DPartitioner(rangex: Int,rangey: Int, numParts:Int) extends Partitioner{

  def numPartitions: Int = numParts

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def getPartition(key: Any): Int = key match {

    case null => 0

    case _ =>
      val point=key.asInstanceOf[Point] //get an new entry
      val x=point.x
      val y=point.y
      val ceilSqrtNumParts = math.ceil(math.sqrt(numParts)).toInt

      val rowid=x/(rangex/ceilSqrtNumParts)
      val columnid=y/(rangey/ceilSqrtNumParts)
      ((rowid*ceilSqrtNumParts+columnid)%numParts).toInt

  }

  override def hashCode: Int = numPartitions

}

/**
 *quadtree based data partition approach
 */
class QuadtreePartitioner(rangex: Int,rangey: Int, numParts:Int) extends Partitioner{

  def numPartitions: Int = numParts

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def getPartition(key: Any): Int = key match {

    case null => 0

    case _ =>
      val point=key.asInstanceOf[Point] //get an new entry
    val x=point.x
      val y=point.y
      val ceilSqrtNumParts = math.ceil(math.sqrt(numParts)).toInt

      val rowid=x/(rangex/ceilSqrtNumParts)
      val columnid=y/(rangey/ceilSqrtNumParts)
      ((rowid*ceilSqrtNumParts+columnid)%numParts).toInt

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
