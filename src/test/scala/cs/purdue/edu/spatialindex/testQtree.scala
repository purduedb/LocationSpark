package cs.purdue.edu.spatialindex

import cs.purdue.edu.spatialbloomfilter.SBFilter
import cs.purdue.edu.spatialindex.quatree.QTree
import cs.purdue.edu.spatialindex.rtree.Box

/**
 * Created by merlin on 10/24/15.
 */
object testQtree {

  def main(args: Array[String]): Unit = {


    /*val quertbox3=Box(0,0,5,5)
    val quertbox2=Box(1,1,10,10)
    val quertbox4=Box(1,1,100,100)
    val quertbox5=Box(100,100,122,133)

    val boxs=Array(quertbox2,quertbox3,quertbox4,quertbox5)

    val qtree=QTree(boxs.toIterator)*/

    val quertbox6= Box(2,2,200,200)

    val quertbox5=Box(220,220,400,433)

    val qtree=QTree(quertbox6)

    qtree.insertBox(quertbox5)

    qtree.printTreeStructure()

    val querybox=Box(120,120,390,390)

    println(qtree.queryBox(querybox))

    println(qtree.queryBoxWithP(querybox))

    val sbfilter=SBFilter(qtree.getSBFilter())

    println(sbfilter.searchRectangle(querybox))

    println(sbfilter.searchRectangleWithP(querybox))

  }

}
