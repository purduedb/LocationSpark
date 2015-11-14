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
        require(math.abs(point.x)<=rangex/2&&math.abs(point.y)<=rangey/2)
        val rowid=((point.x+rangex/2)/(num_col_part)).toInt
        val columnid=((point.y+rangey/2)/(num_row_part)).toInt
        ((rowid*ceilSqrtNumParts+columnid)%numParts)

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
class Grid2DPartitionerForBox(rangex: Int,rangey: Int, numParts:Int) extends Partitioner{

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


  def getPartition(key:Any):Int={
    key match
    {
      case point:Point=>

        require(math.abs(point.x)<=rangex/2&&math.abs(point.y)<=rangey/2)
        //if(math.abs(point.x)<=rangex/2&&math.abs(point.y)<=rangey/2)
          val rowid=((point.x+rangex/2)/(num_col_part)).toInt
          val columnid=((point.y+rangey/2)/(num_row_part)).toInt
          getPartitionIDForIndex(rowid,columnid)

    }

  }


  def getPartitionsForBox(key: Any): HashSet[Int] = key match {

    case None => null

    case box:Box =>
      //val box=key.asInstanceOf[Box] //get an new entry
      require(math.abs(box.x)<=rangex&&math.abs(box.y)<=rangey)
      require(math.abs(box.x2)<=rangex&&math.abs(box.y2)<=rangey)

      val x1=((box.x+rangex/2)/(num_col_part)).toInt
      val y1=((box.y+rangey/2)/(num_row_part)).toInt

      val x2=((box.x2+rangex/2)/(num_col_part)).toInt
      val y2=((box.y2+rangey/2)/(num_row_part)).toInt

      var pids=new HashSet[Int]

      for(i<-x1 to x2) {
        for (j <- y1 to y2) {
            val id=getPartitionIDForIndex(i,j)
            pids+=id
        }
      }
      pids
  }



  def getPartitionsForRangeQuery(key: Any): HashSet[Point] = key match {

    case None => null

    case box:Box =>
      //val box=key.asInstanceOf[Box] //get an new entry
      require(math.abs(box.x)<=rangex&&math.abs(box.y)<=rangey)
      require(math.abs(box.x2)<=rangex&&math.abs(box.y2)<=rangey)

      val x1=((box.x+rangex/2)/(num_col_part)).toInt
      val y1=((box.y+rangey/2)/(num_row_part)).toInt

      val x2=((box.x2+rangex/2)/(num_col_part)).toInt
      val y2=((box.y2+rangey/2)/(num_row_part)).toInt

      var pids=new HashSet[Point]

      for(i<-x1 to x2) {
        for (j <- y1 to y2) {
          //val id=getPartitionIDForIndex(i,j)
          pids+=Point(i*num_row_part-rangex/2, j*num_col_part-rangey/2)
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
