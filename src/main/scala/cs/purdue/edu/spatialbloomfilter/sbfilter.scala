package cs.purdue.edu.spatialbloomfilter

import cs.purdue.edu.spatialindex.rtree.Box

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Quadtree coding order
 * NW,NE,SE,SW
 */

/**
 *this is a adpative spatial bloom filter, it accept a range query, and return whether there are data points inside that range
 */
class sbfilter() {

//define the global int array
  private var internal=new Array[Int](10) //where 10 is the default size

  private var leaf=new Array[Int](10) //where 10 is the default size

  private var budgetsize=100 //100*32

  private var depth=0

  private val widthInternal=ArrayBuffer[Int]() //number of internal node for that depth

  private val widthLeaf=ArrayBuffer[Int]() //number of leaf nodes

  def this(datasize:Int){
   this
    internal = new Array[Int](datasize)
    leaf= new Array[Int](datasize)
  }

  def this(datasize:Int, spaceUsage:Int)
  {
    //this
    this(datasize)
    budgetsize=spaceUsage
  }

  //insert
  /**
   * this operation happen once there is a false positive error
   * insert on rectangle into the index, and mark the covered leaf node as false
   * This is the naive array based solution, we need to insert a box, until  one of its boundary is reached
   * we can use the linked list array solution for update, and copy those data to a bitarray later for effecient query
   */
  def insertRectangle(space: Box): Unit =
  {

    //option1: use one quadtree to insert the query range, then bfs this new quadtree to get its binary code

    //option2: directly work on the bitarray, this is not a optimal solution


  }



  //search
  /**
   * if this rectangle contain data, return true. or return false
   * @return
   */
  def searchRectangle(space: Box):Boolean=
  {

    case class tmpnode(depth:Int, beginBitLocation:Int, parentBox:Box)

    var booleanresult=false
    var currentdepth=0

    //use dfs to search the rectangle, since once we find it has contain, we return true
    val beginlocations = mutable.Stack[tmpnode]()

    beginlocations.push(tmpnode(0,0,qtreeUtil.wholespace))

    while(beginlocations.size>0&&currentdepth<this.depth)
    {
        val point=beginlocations.pop()
        //currentdepth=getDepth(point)

        //get the binary code for this node
        val binnarycode =binnaryopt.GetBytes(
          point.beginBitLocation,
          point.beginBitLocation+qtreeUtil.binnaryUnit,
          this.internal)

      /********************************************/
      //find the first leaf and internal node bit location
      //val internalwidth=
      var InterNodeBitLocation=0
      var LeafNodeBitLocation=0

      if(point.depth!=0)
      {
        val pre=this.widthInternal.take(point.depth-1).sum

        //this step might spend more time
        val numberOnesTobound=binnaryopt.getSetcount(pre*qtreeUtil.binnaryUnit,point.beginBitLocation,this.internal)

        val numberZerosToBound=point.beginBitLocation-pre*qtreeUtil.binnaryUnit-numberOnesTobound

         InterNodeBitLocation=(pre+this.widthInternal(point.depth)+numberOnesTobound)*qtreeUtil.binnaryUnit

         LeafNodeBitLocation=  this.widthLeaf.take(point.depth-1).sum+numberZerosToBound
      }

      /********************************************/

      val tmpbox=point.parentBox
      var flag=1
      var count=0
      var leafcount=0
      var intercount=0

      //"and" operation to know the correct bitcode
      //this cost is constant
        while(count<qtreeUtil.binnaryUnit)
        {

          if((flag&binnarycode)==0)
          //go to the leaf node
          {
              //find leaf relate binary code
              val bitvalueofleaf=binnaryopt.GetBit(LeafNodeBitLocation+leafcount,this.leaf)

              if(bitvalueofleaf==1)    //if this node is true, we return true
              {
                  return true
              }

            leafcount=leafcount+1

          }else
          // go to the internal node
          {
              //judge which subrectangle to prop
              val subrectangle=GetSubRectangle(point.parentBox,count)

              if(IntersectionForTwoBox(subrectangle,space))
              {
                beginlocations.push(tmpnode(point.depth+1,InterNodeBitLocation+intercount*qtreeUtil.binnaryUnit,subrectangle))
              }
              //find the children node location from the binary code
            intercount=intercount+1
          }

          count=count+1
          flag=flag<<1

        }//go through four bit children

      currentdepth=point.depth
    }//while for dfs

    booleanresult
  }

  /**
   * check two box intersect or not
   * @param box1
   * @param box2
   * @return
   */
  def IntersectionForTwoBox(box1:Box,box2:Box):Boolean={

    !(box1.x > box2.x ||
      (box1.x + box1.x2) < box2.x2 ||
      box1.y > box2.y ||
      (box1.y + box1.y2) < box2.y2)
  }

  /**
   *
   * @param box
   * @param count 0:nw 1:ne 2:se 3:ne
   * @return
   */
  def GetSubRectangle(box:Box, count:Int):Box={

    count match{
      case 0=> Box(box.x,box.y+(box.y2-box.y)/2,box.x+(box.x2-box.x)/2,box.y2)
      case 1=> Box(box.x+(box.x2-box.x)/2,box.y+(box.y2-box.y)/2,box.x2,box.y2)
      case 2=> Box(box.x+(box.x2-box.x)/2,box.y,box.x2,box.y+(box.y2-box.y)/2)
      case 3=> Box(box.x,box.y,box.x+(box.x2-box.x)/2,box.y+(box.y2-box.y)/2)
    }

  }



  /**
   * merge nodes when the space budge for the index is over
   */
  def mergeNodes():Unit={

  }

  //

}

object sbfilter
{

  def apply(size:Int)={new sbfilter(size)}
}
