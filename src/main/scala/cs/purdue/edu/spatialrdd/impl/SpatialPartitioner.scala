package cs.purdue.edu.spatialrdd.impl

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.quatree.{QtreeForPartion}
import org.apache.spark.Partitioner
import cs.purdue.edu.spatialindex.rtree._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashSet
import scala.reflect.ClassTag


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
class QtreePartitioner[K: ClassTag,V:ClassTag](partitions:Int, fraction:Float,
                               @transient rdd: RDD[_ <: Product2[K, V]]) extends Partitioner{

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  var realnumPartitions=0

  def numPartitions: Int = {

    if(realnumPartitions!=0&&realnumPartitions<=partitions)
    {
      realnumPartitions
    }else
    {
      partitions
    }

  }

  val quadtree:QtreeForPartion={

    val total=rdd.count()

    var fraction2=fraction

    if(total*fraction>5e5)
    {
      fraction2=(5e5/total).toFloat
    }

    var sampledata=rdd.map(_._1).sample(false,fraction2).collect()

    //in case the sample data size is too small,expand the sample ratio 50 times.
    if(sampledata.length<10000)
    {
      sampledata=rdd.map(_._1).sample(false,0.2).collect()
    }

    var leafbound=sampledata.length/partitions

    if(leafbound==0)
    {
      leafbound=qtreeUtil.leafbound
    }

    val qtree=new QtreeForPartion(leafbound)

    sampledata.foreach{
      case p:Point=>
        qtree.insertPoint(p)

      case _=>println("do not support this data type")
    }

    realnumPartitions=qtree.computePIDofLeaf(sampledata.length,partitions)
    //println("bound "+leafbound)
    //qtree.printTreeStructure()

    qtree
  }


  def getPartition(key: Any): Int = key match {
    case p:Point =>
      this.quadtree.getPID(p)
  }

  /**
   * get the overlap region for the input box
   * @param box
   * @return
   */
  def getPartitionForBox(box:Any):HashSet[Int]=
  {
    box match {
      case box: Box =>
        this.quadtree.getPIDforBox(box)

      case _ =>
        println("do not support other data type now")
        null
    }
  }

  def getPointsForSJoin(box:Any):HashSet[Point]={

    box match {
      case box: Box =>
        this.quadtree.getPointForRangeQuery(box)

      case _ =>
        println("do not support other data type now")
        null
    }
  }

  override def hashCode: Int = realnumPartitions

}


class QtreePartitionerBasedQueries[K: ClassTag,V:ClassTag](partitions:Int,quadtree:QtreeForPartion) extends Partitioner{

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case p:Point =>
      this.quadtree.getPID(p)
  }

  /**
   * get the overlap region for the input box
   * @param box
   * @return
   */
  def getPartitionForBox(box:Any):HashSet[Int]=
  {
    box match {
      case box: Box =>
        this.quadtree.getPIDforBox(box)

      case _ =>
        println("do not support other data type now")
        null
    }
  }

  def getPointsForSJoin(box:Any):HashSet[Point]={

    box match {
      case box: Box =>
        this.quadtree.getPointForRangeQuery(box)

      case _ =>
        println("do not support other data type now")
        null
    }
  }

  override def hashCode: Int = partitions

}

/*private[spark] object QuadtreePartitioner {

  /** Fast multiplicative hash with a nice distribution.
    */
  def byteswap32(v: Int): Int = {
    var hc = v * 0x9e3775cd
    hc = java.lang.Integer.reverseBytes(hc)
    hc * 0x9e3775cd
  }

  /**
   * Reservoir sampling implementation that also returns the input size.
   *
   * @param input input size
   * @param k reservoir size
   * @param seed random seed
   * @return (samples, input size)
   */
  def reservoirSampleAndCount[T: ClassTag](
                                            input: Iterator[T],
                                            k: Int,
                                            seed: Long = Random.nextLong())
  : (Array[T], Int) = {
    val reservoir = new Array[T](k)
    // Put the first k elements in the reservoir.
    var i = 0
    while (i < k && input.hasNext) {
      val item = input.next()
      reservoir(i) = item
      i += 1
    }

    // If we have consumed all the elements, return them. Otherwise do the replacement.
    if (i < k) {
      // If input size < k, trim the array to return only an array of input size.
      val trimReservoir = new Array[T](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      (trimReservoir, i)
    } else {
      // If input size > k, continue the sampling process.
      val rand = new XORShiftRandom(seed)
      while (input.hasNext) {
        val item = input.next()
        val replacementIndex = rand.nextInt(i)
        if (replacementIndex < k) {
          reservoir(replacementIndex) = item
        }
        i += 1
      }
      (reservoir, i)
    }
  }


  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K : ClassTag](rdd: RDD[K], sampleSizePerPartition: Int):
  (Long, Array[(Int, Int, Array[K])]) = {
    val shift = rdd.id

    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()

    val numItems = sketched.map(_._2.toLong).sum
    (numItems, sketched)

  }

}*/

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
