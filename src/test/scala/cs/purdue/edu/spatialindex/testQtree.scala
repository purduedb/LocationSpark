package cs.purdue.edu.spatialindex

import cs.purdue.edu.spatialindex.quatree.QTree
import cs.purdue.edu.spatialindex.rtree.Box

/**
 * Created by merlin on 10/24/15.
 */
object testQtree {

  def main(args: Array[String]): Unit = {

    val querybox=Box(0,0,2,2)

    val quertbox3=Box(0,0,5,5)

    val qtree=QTree(quertbox3)

    val quertbox2=Box(0,0,7,7)
    //qtree.insertBox(quertbox3)
    qtree.insertBox(quertbox2)

    qtree.printTreeStructure()

  }

}
