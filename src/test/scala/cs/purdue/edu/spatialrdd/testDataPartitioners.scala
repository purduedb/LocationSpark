package cs.purdue.edu.spatialrdd

import cs.purdue.edu.spatialrdd.impl._
import cs.purdue.edu.spatialindex.rtree._
/**
 * Created by merlin on 9/23/15.
 */
object testDataPartitioners {

  def main(args: Array[String]): Unit = {

    def testGrid(): Unit =
    {
      val gridpartitioner=new Grid2DPartitioner(100,100,9)

      val point=Point(90,60)

      val pid=gridpartitioner.getPartition(point)

      println(pid)
    }

    val box =  Box(0 , 2, 90, 90)

    val boxpartitioner=new Grid2DPartitionerForBox(100,100,9)

    boxpartitioner.getPartitions(box).foreach(println)
  }
}
