package cs.purdue.edu.spatialrdd

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialrdd.impl._
import cs.purdue.edu.spatialindex.rtree._
/**
 * Created by merlin on 9/23/15.
 */
object testDataPartitioners {

  def main(args: Array[String]): Unit = {

    def testGrid(): Unit =
    {
      val gridpartitioner=new Grid2DPartitioner(90,90,9)

      val point=Point(-15,-15)

      val pid=gridpartitioner.getPartition(point)

      println(pid)
    }

    testGrid()
    println("x"*100)

    val box =  Box(1945,1529, 2296,1684)

    val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangy,9)

    boxpartitioner.getPartitionsForBox(box).foreach(println)

    boxpartitioner.getPartitionsForRangeQuery(box).foreach(println)

  }
}
