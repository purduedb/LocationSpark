package cs.purdue.edu.spatialindex.quatree

import cs.purdue.edu.spatialindex.rtree.Box

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 10/15/15.
 */
abstract class Node(space:Box)
{
  var parent:Node=null
  var belong:Int=0
  def getbox:Box=this.space
}

case class Leaf(space:Box) extends Node(space) {

  var flag=true

  /**
   * spilit a leaf node, and return branch with leaf node
   */
  def spilitLeafNode:Branch={

    val x = this.space.x
    val y = this.space.y

    val hw = ((this.space.x2-this.space.x) / 2)
    val hh = ((this.space.y2-this.space.y) / 2)

    val branch=Branch(this.space)

    branch.nw=Leaf(Box(x,y+hh,x+hw,this.space.y2))
    branch.ne=Leaf(Box(x + hw, y+hh, this.space.x2, this.space.y2))
    branch.se=Leaf(Box(x,y+hh,x+hw,this.space.y2))
    branch.sw=Leaf(Box(x, y, x+hw, y+hh))

    branch.nw.parent=branch
    branch.ne.parent=branch
    branch.se.parent=branch
    branch.sw.parent=branch

    branch
  }

}

/**
 * internal node
 */
case class Branch(space:Box) extends Node(space){

  //pointer for each node
  var nw:Node=null
  var ne:Node=null
  var sw:Node=null
  var se:Node=null

  var bcode:Int=0
  var vcount:Int=0

  /**
   *find child node intersect with the query box
   */
  def findChildNodes(querybox:Box):Iterator[Node]={

    val iter=new ArrayBuffer[Node]

    if(this.nw.getbox.intersects(querybox))
    {
      iter.+=(this.nw)

    }

    if(this.ne.getbox.intersects(querybox))
    {
      iter.+=(this.ne)
    }

    if(this.sw.getbox.intersects(querybox))
    {
      iter.+=(this.sw)
    }

    if(this.se.getbox.intersects(querybox))
    {
      iter.+=(this.se)
    }

    iter.toIterator
  }

}