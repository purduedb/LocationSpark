package cs.purdue.edu.spatialindex.quatree

import cs.purdue.edu.spatialindex.rtree.{Geom, Box}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 10/15/15.
 */
abstract class Node(space:Box) extends Serializable
{
  var parent:Node=null
  var belong:Int=0
  def getbox:Box=this.space
}

case class Leaf(space:Box) extends Node(space) {

  var flag=true
  var count=0
  var id=0

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
    branch.se=Leaf(Box(x+hw,y,this.space.x2,this.space.y+hh))
    branch.sw=Leaf(Box(x, y, x+hw, y+hh))

    branch.nw.parent=branch
    branch.ne.parent=branch
    branch.se.parent=branch
    branch.sw.parent=branch

    branch
  }

}

/**
 * this class is used for bracnhing with count
 * @param space
 */
class leafwithcount(override val space:Box) extends Leaf(space)
{
  var countnw=0
  var countne=0
  var countse=0
  var countsw=0

   def updatecount(p:Geom) =
  {
    val x = this.space.x
    val y = this.space.y

    val hw = ((this.space.x2-this.space.x) / 2)
    val hh = ((this.space.y2-this.space.y) / 2)

    if(p.x<hw&&p.y<hh)
    {
      this.countsw+=1
    }else if(p.x>=hw&&p.y<hh)
    {
      this.countse+=1
    }else if(p.x<hw&&p.y>=hh)
    {
      this.countnw+=1
    }else if(p.x>=hw&&p.y>=hh)
    {
      this.countne+=1
    }

  }

  /**
   * spilit a leaf node, and return branch with leaf node
   */
  override  def spilitLeafNode:Branch={

    val x = this.space.x
    val y = this.space.y

    val hw = ((this.space.x2-this.space.x) / 2)
    val hh = ((this.space.y2-this.space.y) / 2)

    val branch=Branch(this.space)
    val nw=new leafwithcount(Box(x,y+hh,x+hw,this.space.y2))
    nw.count=this.countnw
    branch.nw=nw

    val ne=new leafwithcount(Box(x + hw, y+hh, this.space.x2, this.space.y2))
    ne.count=this.countne
    branch.ne=ne

    val sw=new leafwithcount(Box(x, y, x+hw, y+hh))
    sw.count=this.countsw
    branch.sw=sw

    val se=new leafwithcount(Box(x+hw,y,this.space.x2,this.space.y+hh))
    nw.count=this.countse
    branch.se=se

    branch.nw.parent=branch
    branch.ne.parent=branch
    branch.se.parent=branch
    branch.sw.parent=branch
    //branch.vcount=this.count
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
  def findChildNodes(querybox:Geom):Iterator[Node]={

    val iter=new ArrayBuffer[Node]

    if(this.nw.getbox.intersects(querybox))
    {
      iter.+=(this.nw)

    }

    if(this.ne.getbox.intersects(querybox))
    {
      iter.+=(this.ne)
    }

    if(this.se.getbox.intersects(querybox))
    {
      iter.+=(this.se)
    }

    if(this.sw.getbox.intersects(querybox))
    {
      iter.+=(this.sw)
    }

    iter.toIterator
  }

  /**
   *find child node intersect with the query box
   */
  def findChildFlag():Boolean={

    var ret=true

    this.nw match{
      case l:Leaf=>
        ret=ret&&l.flag
        if(ret==true)
          return ret
    }

    this.ne match{
      case l:Leaf=>
        ret=ret&&l.flag
        if(ret==true)
          return ret
    }

    this.se match{
      case l:Leaf=>
        ret=ret&&l.flag
        if(ret==true)
          return ret
    }

    this.sw match{
      case l:Leaf=>
        ret=ret&&l.flag
        if(ret==true)
          return ret
    }

    ret

  }


}