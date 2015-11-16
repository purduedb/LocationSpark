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
      val gridpartitioner=new Grid2DPartitioner(qtreeUtil.rangx,qtreeUtil.rangy,4)

      val point=Point(30.40094f,-86.8612f)

      val pid=gridpartitioner.getPartition(point)

      println(pid)
    }

    testGrid()

    println("x"*100)

    val box =Box(30.10094f,-86.8612f, 32.41f, -80.222f)

    val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangy,4)

    boxpartitioner.getPartitionsForBox(box).foreach(println)

    boxpartitioner.getPartitionsForRangeQuery(box).foreach(println)

  }
}
