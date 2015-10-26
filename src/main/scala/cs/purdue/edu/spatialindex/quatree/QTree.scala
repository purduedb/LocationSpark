package cs.purdue.edu.spatialindex.quatree

import cs.purdue.edu.spatialbloomfilter.{qtreeUtil, binnaryopt}
import cs.purdue.edu.spatialindex.rtree.Box

import scala.collection.mutable.ArrayBuffer

/**
 * Created by merlin on 10/15/15.
 */
/**
 * this Qtree only support insert rectangle and each leaf node only contain boolean value
 */
/**
 * the spilit leaf box order is NW,NE,SE,SW
 */

case class QTree () {

  var budget=0
  var root:Node=null

  def this(size:Int)
  {
      this
      budget=size
  }

  def this(root:Node)
  {
    this(1000)
    this.root=root
  }

  /**
   * alignEdge=1011 is the binary code to require which edge to algin with quadtree block
   * the order is North, east, south, west,
   */
   case class tmpBox(alignEdge:Int, inputbox:Box)

  /**
   * insert a box only if the false negative happen
   * @param space
   */
  def insertBox(space: Box):Unit={


    val querybox=tmpBox(15,space)

    insertBox(this.root,querybox)

  }

  /**
   * insert a box if the true postive happen
   * @param space
   */
  def insertTBox(space: Box)={

  }

  /**
   *insert the box into quadtree, and mark the leaf node as false
   */
  private def insertBox(parent:Node, space: tmpBox): Unit ={

    if(!parent.getbox.contains(space.inputbox))
    {
      return
    }

      parent match {
        case l: Leaf=>

              if(l.flag==false&&l.getbox.contains(space.inputbox))
              {
                return
              }

            //check which edge of leaf coalgin with the current box
              if(coalignblock(space,l)||space.inputbox.contains(l.getbox))
              {
                //if all the required edge is matched or the input box contain the leaf node return
                l.flag=false
                return
              }else
              {
                //spilit the current leaf node and replace with an internal node
                val branch=l.spilitLeafNode

                println("spilit this leaf node: "+l.getbox.toString)

                l.parent match{
                  case parent:Branch=>
                    //set up the pointer for this new node
                    if(l.equals(parent.ne))
                    {
                      parent.ne=branch
                    }else if(l.equals(parent.nw))
                    {
                      parent.nw=branch
                    }else if(l.equals(parent.se))
                    {
                      parent.se=branch
                    }else if(l.equals(parent.sw))
                    {
                      parent.sw=branch
                    }

                }

                // spilit the current query box
                val spilitbox=spilitQueryBox(space,l)

                spilitbox.foreach{
                  newnode=>
                    findoneSubbox(branch, newnode.inputbox).foreach(
                    overlapnode=>insertBox(overlapnode,newnode)
                    )
                }

                /*for( i<-0 to spilitbox.length-1 ) {
                  //continue the insert for each leaf node of the current
                  if(spilitbox(i)!=null&&spilitbox(i).inputbox!=null)
                  {
                    i match {
                      case 0 =>  insertBox(branch.nw, spilitbox(i))
                      case 1 =>  insertBox(branch.ne, spilitbox(i))
                      case 2 =>  insertBox(branch.se, spilitbox(i))
                      case 3 =>  insertBox(branch.sw, spilitbox(i))
                    }
                  }

                }*/
                //continue the search the sub box

              }//else

        case b:Branch=>

          var indicator=false
          b.findChildNodes(space.inputbox).foreach{
            children=>
              //if all the required edge algin
              if(space.inputbox.contains(children.getbox)||coalignblock(space,children))
              {
                //mark all his children leaf node as false
                markleafNode(b,space.inputbox)
              }else
              {
                indicator=true
              }
          }

          //we need to go for subbranch searching
          if(indicator)
          {
            val spilitbox=spilitQueryBox(space,b)

            spilitbox.foreach{
              newnode=>
                findoneSubbox(b,newnode.inputbox).foreach(
                  overlapnode=>insertBox(overlapnode,newnode)
                )
            }

            /*for( i<-0 to spilitbox.length-1 ) {

              if(spilitbox(i)!=null&&spilitbox(i).inputbox!=null)
              {
                //continue the insert for each leaf node of the current
                i match {
                  case 0 =>  if(!(coalignblock(space,b.nw)&&space.inputbox.contains(b.nw.getbox))) insertBox(b.nw, spilitbox(i))
                  case 1 =>  if(!(coalignblock(space,b.ne)&&space.inputbox.contains(b.ne.getbox))) insertBox(b.ne, spilitbox(i))
                  case 2 =>  if(!(coalignblock(space,b.se)&&space.inputbox.contains(b.se.getbox))) insertBox(b.se, spilitbox(i))
                  case 3 =>  if(!(coalignblock(space,b.sw)&&space.inputbox.contains(b.sw.getbox))) insertBox(b.sw, spilitbox(i))
                }

              }

            }*/
            //for

          }//if indicator

      }

    }


  /**
   *find child node intersect with the query box
   */
  def findoneSubbox(branch:Branch, querybox:Box):Iterator[Node]={

    val iter=new ArrayBuffer[Node]

    if(branch.nw.getbox.intersects(querybox))
    {
      iter.+=(branch.nw)
    }else if(branch.ne.getbox.intersects(querybox))
    {
      iter.+=(branch.ne)
    } else if(branch.sw.getbox.intersects(querybox))
    {
      iter.+=(branch.sw)
    } else if(branch.se.getbox.intersects(querybox))
    {
      iter.+=(branch.se)
    }

    iter.toIterator
  }

  /**
   *spilt the query box into sub rectangles
   * return order as
   */
  def spilitQueryBox(input:tmpBox,qnode:Node):Array[tmpBox]={

       val tmpboxs =new Array[tmpBox](4)

       val midx= (qnode.getbox.x+  qnode.getbox.x2)/2
       val midy= (qnode.getbox.y+  qnode.getbox.y2)/2

       val code=input.alignEdge
       val inputbox=input.inputbox

    //case 3
    if(inputbox.x<midx&&inputbox.y<midy&&inputbox.x2>midx&&inputbox.y2>midy)
    {
      val a=Box(inputbox.x,midy, midx,inputbox.y2)
      var secode=binnaryopt.clearBit(1,code)
      secode= binnaryopt.clearBit(2,secode)
      tmpboxs(0)=tmpBox(secode,a)

      val b=Box(midx,midy, inputbox.x2,inputbox.y2)
      var bcode=binnaryopt.clearBit(2,code)
      bcode=binnaryopt.clearBit(3,code)
      tmpboxs(1)=tmpBox(bcode,b)

      val c=Box(midx,inputbox.y, inputbox.x2,midy)
      var ccode=binnaryopt.clearBit(0,code)
      ccode=binnaryopt.clearBit(3,ccode)
      tmpboxs(2)=tmpBox(ccode,c)

      val d=Box(inputbox.x,inputbox.y, midx,midy)
      var dcode=binnaryopt.clearBit(0,code)
      dcode=binnaryopt.clearBit(1,code)
      tmpboxs(3)=tmpBox(dcode,d)
      return  tmpboxs
    }

    //case 1, for the query box only lay to one subbox
    if(inputbox.x2<midx&&inputbox.y>midy)
    {//nw
      tmpboxs(0)=input
      return tmpboxs
    }else if(inputbox.x>=midx&&inputbox.y>midy)
    {//ne
      tmpboxs(1)=input
      return tmpboxs
    }else if(inputbox.x>midx&&inputbox.y2<=midy)
    { //se
      tmpboxs(2)=input
      return tmpboxs
    }else if(inputbox.x2<midx&&inputbox.y2<=midy)
    { //sw
      tmpboxs(3)=input
      return tmpboxs
    }


    // case 2: for query box lays to two subboxes
    if(inputbox.x<midx&&inputbox.y>midy&&inputbox.x2>midx&&inputbox.y2>midy)
    {     //nw and ne
           val nw=Box(inputbox.x,inputbox.y, midx,inputbox.y2)
           val nwcode=binnaryopt.clearBit(1,code)
           tmpboxs(0)=tmpBox(nwcode,nw)

           val ne=Box(midx,inputbox.y, inputbox.x2,inputbox.y2)
           val necode=binnaryopt.clearBit(3,code)
           tmpboxs(1)=tmpBox(necode,ne)
            return tmpboxs

    }else if(inputbox.x>=midx&&inputbox.y<=midy&&inputbox.x2>midx&&inputbox.y2>midy)
    {       //ne and se

            val ne=Box(inputbox.x,midy, inputbox.x2,inputbox.y2)
            val necode=binnaryopt.clearBit(2,code)
            tmpboxs(1)=tmpBox(necode,ne)

            val se=Box(inputbox.x,inputbox.y, inputbox.x2,midy)
            val secode=binnaryopt.clearBit(0,code)
            tmpboxs(2)=tmpBox(secode,se)

            return tmpboxs

    }else if(inputbox.x<midx&&inputbox.y<=midy&&inputbox.x2>midx&&inputbox.y2<=midy)
    {       //sw and se

            val se=Box(midx,inputbox.y, inputbox.x2,inputbox.y2)
            val secode=binnaryopt.clearBit(3,code)
            tmpboxs(2)=tmpBox(secode,se)

            val ne=Box(inputbox.x,inputbox.y, midx,inputbox.y2)
            val necode=binnaryopt.clearBit(1,code)
            tmpboxs(3)=tmpBox(necode,ne)
            return tmpboxs

    } else if(inputbox.x<midx&&inputbox.y<=midy&&inputbox.x2<midx&&inputbox.y2>midy)
    {       //sw and nw

            val ne=Box(inputbox.x,inputbox.y, inputbox.x2,midy)
            val necode=binnaryopt.clearBit(0,code)
            tmpboxs(3)=tmpBox(necode,ne)

            val se=Box(inputbox.x,midy, inputbox.x2,inputbox.y2)
            val secode=binnaryopt.clearBit(3,code)
            tmpboxs(0)=tmpBox(secode,se)
            return tmpboxs
    }

    tmpboxs
     //case 3: for the query box, lay to four boxes

  }


  /**
   *check weather the current space coalgin any the boundary of the edge
   */
  def coalignblock(space: tmpBox , qnode:Node):Boolean={

      //val value=space.alignEdge

    var count=0

    while(count<4)
    {
      //we need to algin on this edge
      if(binnaryopt.getBit(count,space.alignEdge)==1)
      {
         //check algin or not
         count match {
           case 0=>
                if(Math.abs(space.inputbox.y2-qnode.getbox.y2)>0.1)
                    return false
           case 1=>
                if(Math.abs(space.inputbox.x2-qnode.getbox.x2)>0.1)
                     return false
           case 2=>
                if(Math.abs(space.inputbox.y-qnode.getbox.y)>0.1)
                   return false
           case 3=>
                if(Math.abs(space.inputbox.x-qnode.getbox.x)>0.1)
                    return false
         }
      }

      count=count+1
    }

    true
  }

  /**
   * mark this node's sub children as false
   * @param node
   * @param box
   */
  def markleafNode(node:Node, box:Box):Boolean={

    if(!box.contains(node.getbox))
    {
      return false
    }

      node match {
        case l:Leaf=>
            l.flag=false
            true
        case b:Branch=>
            markleafNode(b.ne,box)
            markleafNode(b.nw,box)
            markleafNode(b.se,box)
            markleafNode(b.sw,box)
          true
      }

  }


  /**
   *merge some nodes with no visit information
   */
    def mergeNodes()={

    }


  /**
   *
   */
    def count():Int={

      0
    }

  /**
   *get the binary code of the quadtree
   */
    def serilization():Int={

     0
    }


  /**
   * println the quadtree structure
   */
  def printTreeStructure(): Unit =
  {

    //val queue:Queue[Node]=new Queue()

    val queue = new scala.collection.mutable.Queue[Node]

    queue+=this.root

    var front=1
    var end=queue.length
    var depth=1

    while(!queue.isEmpty)
    {

      val pnode=queue.dequeue()

      pnode match
      {
        case l:Leaf =>
          print(" L("+l.flag+") ")

        case b:Branch =>
          queue.enqueue(b.ne)
          queue.enqueue(b.nw)
          queue.enqueue(b.se)
          queue.enqueue(b.sw)
          printf(" B ")
      }

      front=front+1

      if(front>end)
      {
        depth=depth+1
        println("\n--------------------------------------------------")
        front=1
        end=queue.length
      }

    }

    println("depth is "+depth)

  }


}

object QTree{

  /**
   * Construct an empty QTree.
   */
  def empty:QTree = {
    val leaf=Leaf(qtreeUtil.wholespace)
    val branch=leaf.spilitLeafNode
    new QTree(branch)
  }

  /**
   * build a QTree from a query box
   * @param querybox
   */
  def apply(querybox:Box):QTree=
  {
    val qtree=QTree.empty
    qtree.insertBox(querybox)
    qtree
  }

  /**
   * Construct an QTree from a sequence of entries.
   */
  def apply(itr:Iterator[Box]):QTree=
  {
    val qtree=QTree.empty
    itr.foreach{ box=>qtree.insertBox(box) }
    qtree
  }

}
