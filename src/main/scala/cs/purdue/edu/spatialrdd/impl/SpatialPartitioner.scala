package cs.purdue.edu.spatialrdd.impl

import org.apache.spark.Partitioner
import cs.purdue.edu.spatialindex.rtree._

import scala.collection.mutable.HashSet

/**
 * static data partition approach
 */
class Grid2DPartitioner(rangex: Int,rangey: Int, numParts:Int) extends Partitioner{

  def numPartitions: Int = numParts
  def ceilSqrtNumParts = math.ceil(math.sqrt(numParts)).toInt
  def num_row_part=rangex/ceilSqrtNumParts
  def num_col_part=rangey/ceilSqrtNumParts

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def getPartition(key: Any): Int = key match {

    case None => 0

    case point:Point =>
      //val point=key.asInstanceOf[Point] //get an new entry
      val rowid=(point.x/(num_col_part)).toInt
      val columnid=(point.y/(num_row_part)).toInt

      val tmp=rowid*ceilSqrtNumParts+columnid

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
      key.asInstanceOf[Integer]

  }

  override def hashCode: Int = numPartitions

}

/**
 *quadtree based data partition approach
 */
class Grid2DPartitionerForBox(rangex: Int,rangey: Int, numParts:Int){

  def numPartitions: Int = numParts

  def ceilSqrtNumParts = math.ceil(math.sqrt(numParts)).toInt
  def num_row_part=rangex/ceilSqrtNumParts
  def num_col_part=rangey/ceilSqrtNumParts


  def getPartitionIDForIndex(key: Any): Int = {
    key match {
      case (i: Int, j : Int) => (i * ceilSqrtNumParts + j) % numPartitions
      case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }


  def getPartitionForPoint(key:Any):Int={

    key match
    {
      case p:Point=>
        val x1=(p.x/(num_col_part)).toInt
        val y1=(p.y/(num_row_part)).toInt
        getPartitionIDForIndex(x1,y1)
    }

  }


  def getPartitions(key: Any): HashSet[Int] = key match {

    case None => null

    case box:Box =>
      //val box=key.asInstanceOf[Box] //get an new entry

      val x1=(box.x/(num_col_part)).toInt
      val y1=(box.y/(num_row_part)).toInt

      val x2=(box.x2/(num_col_part)).toInt
      val y2=(box.y2/(num_row_part)).toInt

      var pids=new HashSet[Int]

      for(i<-x1 to x2) {
        for (j <- y1 to y2) {
            val id=getPartitionIDForIndex(i,j)
            pids+=id
        }
      }
      pids

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
