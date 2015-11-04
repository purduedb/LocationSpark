package cs.purdue.edu.spatialbloomfilter

import cs.purdue.edu.spatialindex.rtree.Box

import scala.collection.mutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Quadtree coding order
 * NW,NE,SE,SW
 */

/**
 *this is a adpative spatial bloom filter, it accept a range query, and return whether there are data points inside that range
 */
class SBFilter() {

//define the global int array
  private var internal=new Array[Int](3) //where 10 is the default size

  private var leaf=new Array[Int](3) //where 10 is the default size

  private var budgetsize=100 //100*32

  private var depth=0

  //we will remove this two array later
  //private var widthInternal=ArrayBuffer[Int]() //number of internal node for that depth
  private var widthInternalSum=Array[Int]() //number of internal node for that depth
  //private var widthLeaf=ArrayBuffer[Int]() //number of leaf nodes
  private var widthLeafSum=Array[Int]() //number of leaf nodes

  //this prestorage does not help anymore
  private var Internallocation = scala.collection.mutable.HashMap.empty[Int,Int]
  private var Leaflocation = scala.collection.mutable.HashMap.empty[Int,Int]

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

    var currentdepth=0

    //use dfs to search the rectangle, since once we find it has contain, we return true
    val beginlocations = mutable.Stack[tmpnode]()

    beginlocations.push(tmpnode(0,0,qtreeUtil.wholespace))

    while(beginlocations.size>0&&currentdepth<this.depth)
    {
        val point=beginlocations.pop()

        //get the binary code for this node
        val binnarycode =binnaryopt.getBytes(
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
         val pre=this.widthInternalSum(point.depth-1)
         val numberOnesTobound=binnaryopt.getSetcount(pre*qtreeUtil.binnaryUnit,point.beginBitLocation,this.internal)
         val numberZerosToBound=point.beginBitLocation-pre*qtreeUtil.binnaryUnit-numberOnesTobound
         InterNodeBitLocation=(this.widthInternalSum(point.depth)+numberOnesTobound)*qtreeUtil.binnaryUnit
         LeafNodeBitLocation= this.widthLeafSum(point.depth)+numberZerosToBound
      }else
      {
         InterNodeBitLocation=4
      }

      /********************************************/

      var flag=1
      var count=0
      var leafcount=0
      var intercount=0
      //this cost is constant
        while(count<qtreeUtil.binnaryUnit)
        {
          val subrectangle=GetSubRectangle(point.parentBox,count)

          if((flag&binnarycode)==0)
          {
            if(subrectangle.intersects(space))
             {
               //println("intersect leaf is:"+subrectangle.toString)
                val bitvalueofleaf=binnaryopt.getBit(LeafNodeBitLocation+leafcount,this.leaf)

                if(bitvalueofleaf==1)  //if this leaf node is true, we return true
                {
                  return true

                }else if(subrectangle.contains(space))
                { //if this leaf is false and it contains the input the space, return the false
                  //println("find the false leaf")
                  return false
                }
            }
            leafcount=leafcount+1

          }else  // go to the internal node
          {
              //judge which subrectangle to prop
            if(subrectangle.intersects(space))
            {
              beginlocations.push(tmpnode(point.depth+1,InterNodeBitLocation+intercount*qtreeUtil.binnaryUnit,subrectangle))
            }

            intercount=intercount+1
          }

          count=count+1
          flag=flag<<1

        }//go through four bit children

     // println("current depth "+point.depth)
      currentdepth=point.depth

    }//while for dfs

    false
  }

  //search
  /**
   * if this rectangle contain data, return true. or return false
   * @return
   */
  def searchRectangleV2(space: Box):Boolean=
  {

    case class tmpnode(depth:Int, beginBitLocation:Int, parentBox:Box)

    var currentdepth=0

    //use dfs to search the rectangle, since once we find it has contain, we return true
    val beginlocations = mutable.Stack[tmpnode]()

    beginlocations.push(tmpnode(0,0,qtreeUtil.wholespace))

    while(beginlocations.size>0&&currentdepth<this.depth)
    {
      val point=beginlocations.pop()
      //currentdepth=getDepth(point)

      //get the binary code for this node
      val binnarycode =binnaryopt.getBytes(
        point.beginBitLocation,
        point.beginBitLocation+qtreeUtil.binnaryUnit,
        this.internal)

      //println("beginlocation "+  point.beginBitLocation)
      //println("binnary code "+binnaryopt.getBitString(binnarycode))

      /********************************************/
      //find the first leaf and internal node bit location
      //val internalwidth=
      var InterNodeBitLocation=0
      var LeafNodeBitLocation=0

      if(point.depth!=0)
      {
        /*val pre=this.widthInternalSum(point.depth-1)
        val numberOnesTobound=binnaryopt.getSetcount(pre*qtreeUtil.binnaryUnit,point.beginBitLocation,this.internal)
        val numberZerosToBound=point.beginBitLocation-pre*qtreeUtil.binnaryUnit-numberOnesTobound
        InterNodeBitLocation=(this.widthInternalSum(point.depth)+numberOnesTobound)*qtreeUtil.binnaryUnit
        LeafNodeBitLocation= this.widthLeafSum(point.depth)+numberZerosToBound*/

        InterNodeBitLocation=this.Internallocation.getOrElse(point.beginBitLocation, 0)
        LeafNodeBitLocation=this.Leaflocation.getOrElse(point.beginBitLocation, 0)

      }else
      {
        InterNodeBitLocation=4
      }

      /********************************************/

      var flag=1
      var count=0
      var leafcount=0
      var intercount=0

      //"and" operation to know the correct bitcode
      //this cost is constant
      while(count<qtreeUtil.binnaryUnit)
      {
        val subrectangle=GetSubRectangle(point.parentBox,count)

        if((flag&binnarycode)==0)
        {


          if(subrectangle.intersects(space))
          {
            //println("intersect leaf is:"+subrectangle.toString)
            val bitvalueofleaf=binnaryopt.getBit(LeafNodeBitLocation+leafcount,this.leaf)

            if(bitvalueofleaf==1)  //if this leaf node is true, we return true
            {
              return true

            }else if(subrectangle.contains(space))
            { //if this leaf is false and it contains the input the space, return the false
              //println("find the false leaf")
              return false
            }
          }
          leafcount=leafcount+1

        }else  // go to the internal node
        {
          //judge which subrectangle to prop
          if(subrectangle.intersects(space))
          {
            //println("intersect branch is:"+subrectangle.toString)
            beginlocations.push(tmpnode(point.depth+1,InterNodeBitLocation+intercount*qtreeUtil.binnaryUnit,subrectangle))
          }
          //if(IntersectionForTwoBox(subrectangle,space))
          intercount=intercount+1
        }

        count=count+1
        flag=flag<<1

      }//go through four bit children

      // println("current depth "+point.depth)
      currentdepth=point.depth

    }//while for dfs

    false
  }

  //search
  /**
   * return the possiblity for this input space have data
   * @return
   */
  def searchRectangleWithP(space: Box):Double=
  {

    case class tmpnode(depth:Int, beginBitLocation:Int, parentBox:Box)

    var falseratio=0.0

    var currentdepth=0

    //use dfs to search the rectangle, since once we find it has contain, we return true
    val beginlocations = mutable.Stack[tmpnode]()

    beginlocations.push(tmpnode(0,0,qtreeUtil.wholespace))

    while(beginlocations.size>0&&currentdepth<this.depth)
    {
      val point=beginlocations.pop()

      val binnarycode =binnaryopt.getBytes(
        point.beginBitLocation,
        point.beginBitLocation+qtreeUtil.binnaryUnit,
        this.internal)

      /********************************************/

      var InterNodeBitLocation=0
      var LeafNodeBitLocation=0

      if(point.depth!=0)
      {
        val pre=this.widthInternalSum(point.depth-1)
        //this step might spend more time
        val numberOnesTobound=binnaryopt.getSetcount(pre*qtreeUtil.binnaryUnit,point.beginBitLocation,this.internal)
        val numberZerosToBound=point.beginBitLocation-pre*qtreeUtil.binnaryUnit-numberOnesTobound

        InterNodeBitLocation=(this.widthInternalSum(point.depth)+numberOnesTobound)*qtreeUtil.binnaryUnit
        LeafNodeBitLocation= this.widthLeafSum(point.depth)+numberZerosToBound

      }else
      {
        InterNodeBitLocation=4
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

        val subrectangle=GetSubRectangle(point.parentBox,count)

        if((flag&binnarycode)==0) //go to the leaf node
        {
          if(space.contains(subrectangle))
          {

           // println("leaf location is:"+LeafNodeBitLocation)
            val bitvalueofleaf=binnaryopt.getBit(LeafNodeBitLocation+leafcount,this.leaf)

            if(bitvalueofleaf==0)    //if this node is true, we return true
            {
              //println("contain leaf is:"+subrectangle.toString)
              falseratio+=subrectangle.area
            }

          }

          leafcount=leafcount+1

        }else
        // go to the internal node
        {

          //judge which subrectangle to prop
          if(subrectangle.intersects(space))
          {
             //println("intersect branch is:"+subrectangle.toString)
              beginlocations.push(tmpnode(point.depth+1,InterNodeBitLocation+intercount*qtreeUtil.binnaryUnit,subrectangle))
          }
          //if(IntersectionForTwoBox(subrectangle,space))

          intercount=intercount+1
          //find the children node location from the binary code

        }

        count=count+1
        flag=flag<<1

      }//go through four bit children

      currentdepth=point.depth
    }//while for dfs

    (falseratio/space.area)/(1 - qtreeUtil.leafStopBound)

  }


  /**
   *
   */
  def GetSubRectangle(inputbox:Box, count:Int):Box={

    val midx=(inputbox.x2+inputbox.x)/2
    val midy=(inputbox.y2+inputbox.y)/2

    count match{
      case 0=> Box(inputbox.x, midy, midx, inputbox.y2)
      case 1=> Box(midx, midy, inputbox.x2, inputbox.y2)
      case 2=> Box(midx, inputbox.y, inputbox.x2, midy)
      case 3=> Box(inputbox.x, inputbox.y, midx, midy)
    }

  }



  /**
   * merge nodes when the space budge for the index is over
   * to do list
   */
  def mergeNodes():Unit={

  }



  /*
  def printTree():Unit={

      def printInternal()={

        //this.widthInternal.foldLeft(0)((i,j)=>)

        for(i<-0 to this.widthInternal.length-2)
        {
            binnaryopt.getBitString(this.widthInternal(i), this.widthInternal(i+1), this.internal)
        }

         binnaryopt.getBitString(this.widthInternal(this.widthInternal.length-2), this.widthInternal(this.widthInternal.length-1), this.internal)

      }

      def printLeaf()={

        //this.widthInternal.foldLeft(0)((i,j)=>)
        for(i<-0 to this.widthLeaf.length-2)
        {
           binnaryopt.getBitString(this.widthLeaf(i), this.widthLeaf(i+1), this.leaf)
        }

        binnaryopt.getBitString(this.widthLeaf(this.widthLeaf(this.widthLeaf.length-2)), this.widthLeaf(this.widthLeaf.length-1), this.leaf)
     }

    println("*"*10)
    printInternal()

    println("*"*10)
    printLeaf()

  }*/

  //

}

/**
 * class to transfer data from the local partition to mater node
 */
case class dataSBFV2(size:Int,
                   internal:Array[Int],
                   leaf:Array[Int],
                   widthInternal:ArrayBuffer[Int],
                   widthLeaf:ArrayBuffer[Int],
                   Internallocation:HashMap[Int, Int],
                   Leaflocation:HashMap[Int, Int])  extends Serializable ()

/**
 * class to transfer data from the local partition to mater node
 */
case class dataSBF(size:Int,
                   internal:Array[Int],
                   leaf:Array[Int],
                   widthInternal:ArrayBuffer[Int],
                   widthLeaf:ArrayBuffer[Int])  extends Serializable ()

object SBFilter
{

  def apply(size:Int)={new SBFilter(size)}

  def apply(data:dataSBFV2):SBFilter={
    val sbfilter=new SBFilter(data.size)
    sbfilter.internal=data.internal
    sbfilter.leaf=data.leaf

    sbfilter.depth=data.widthInternal.size

    sbfilter.widthInternalSum=new Array[Int](data.widthInternal.size)
    sbfilter.widthInternalSum(0)=data.widthInternal(0)

    for(i<- 1 to data.widthInternal.size-1)
    {
      sbfilter.widthInternalSum(i)= sbfilter.widthInternalSum(i-1)+data.widthInternal(i)
    }

    sbfilter.widthLeafSum=new Array[Int](data.widthLeaf.size)
    sbfilter.widthLeafSum(0)=data.widthLeaf(0)

    for(i<- 1 to data.widthLeaf.size-1)
    {
      sbfilter.widthLeafSum(i)= sbfilter.widthLeafSum(i-1)+data.widthLeaf(i)
    }

    sbfilter.Internallocation=data.Internallocation
    sbfilter.Leaflocation=data.Leaflocation

    sbfilter
  }

  def apply(data:dataSBF):SBFilter={

    val sbfilter=new SBFilter(data.size)

    sbfilter.internal=data.internal
    sbfilter.leaf=data.leaf
    sbfilter.depth=data.widthInternal.size

    sbfilter.widthInternalSum=new Array[Int](data.widthInternal.size)
    sbfilter.widthInternalSum(0)=data.widthInternal(0)

    for(i<- 1 to data.widthInternal.size-1)
    {
      sbfilter.widthInternalSum(i)= sbfilter.widthInternalSum(i-1)+data.widthInternal(i)
    }

    sbfilter.widthLeafSum=new Array[Int](data.widthLeaf.size)
    sbfilter.widthLeafSum(0)=data.widthLeaf(0)

    for(i<- 1 to data.widthLeaf.size-1)
    {
      sbfilter.widthLeafSum(i)= sbfilter.widthLeafSum(i-1)+data.widthLeaf(i)
    }
    sbfilter
  }

}
