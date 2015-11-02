package cs.purdue.edu.spatialindex.quatree

import cs.purdue.edu.spatialbloomfilter.{binnaryopt, dataSBF, qtreeUtil}
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

case class QTree() {

  var budget = 0
  var root: Node = null

  var currentbox: Box = null

  def this(size: Int) {
    this
    budget = size
    this.root = Leaf(qtreeUtil.wholespace).spilitLeafNode
  }

  def this(root: Node) {
    this(1000)
    this.root = root
  }

  /**
   * insert a box only if the false negative happen
   * @param space
   */
  def insertBox(space: Box): Unit = {

    currentbox = space
    val querybox = tmpBox(15, space)

    insertBox(this.root, querybox)

  }

  /**
   * check whether there are data inside the qspace
   * @param qspace
   * @return
   */
  def queryBox(qspace: Box): Boolean = {

    //find one leaf node false, return false
    queryBox(this.root, qspace)

  }

  /**
   * query and return the conditional possibility for false
   * @param qspace
   * @return
   */
  def queryBoxWithP(qspace: Box): Double = {

    //find one leaf node false, return false
    val sum = Array[Double](1)
    queryBoxWithP(this.root, qspace, sum)
    (sum(0) / qspace.area) / (1 - qtreeUtil.leafStopBound)

  }

  /**
   * insert a box if the true postive happen
   * @param space
   */
  def insertTBox(space: Box) = {

  }

  /**
   * merge some nodes with no visit information
   */
  def mergeNodes() = {


  }

  /**
   *
   */
  def count(): Int = {

    0
  }

  /**
   * get the binary code of the quadtree
   */
  def getSBFilter(): dataSBF = {

    val maxdatasize = 1000

    //define the global int array
    val internal = new Array[Int](maxdatasize) //where 10 is the default size
    val leaf = new Array[Int](maxdatasize) //where 10 is the default size

    var widthInternal = ArrayBuffer[Int]() //number of internal node for that depth
    var widthLeaf = ArrayBuffer[Int]() //number of leaf nodes

    val queue = new scala.collection.mutable.Queue[Node]

    queue += this.root

    var numberLeaf = 0
    var front = 1
    var end = queue.length
    var depth = 1
    var leafLocation = 0
    var internalLocation = 0

    //this is used for the bound number
    var numOnesToBound=0
    var numZerosToBound=0

    val InternalHash = scala.collection.mutable.HashMap.empty[Int,Int]
    val LeafHash = scala.collection.mutable.HashMap.empty[Int,Int]

    while (!queue.isEmpty) {

      val pnode = queue.dequeue()

      pnode match {
        case l: Leaf => {

          if (l.flag) {
            binnaryopt.setBit(leafLocation, leaf)
          }
          else {
            binnaryopt.clearBit(leafLocation, leaf)
          }

          leafLocation += 1
          numberLeaf += 1
        }

        case b: Branch =>
          queue.enqueue(b.nw)
          queue.enqueue(b.ne)
          queue.enqueue(b.se)
          queue.enqueue(b.sw)
          setbitOfBranch(b, internalLocation, internal)
          InternalHash.+=(internalLocation->numOnesToBound*qtreeUtil.binnaryUnit)
          LeafHash.+=(internalLocation->numZerosToBound)

          internalLocation += 4
          numOnesToBound+=getbitOfBranch(b)
          numZerosToBound+=4-getbitOfBranch(b)
          //println("branch: "+b.getbox.toString)
          //println("internal: "+internalLocation)
         // println(binnaryopt.getBitString(0,100,internal))

      }

      front = front + 1

      if (front > end) {
        widthLeaf+=(numberLeaf)
        widthInternal+=(end - numberLeaf)
        depth += 1
        front= 1
        end = queue.size
        numberLeaf = 0
        //
      }

    }

    println("# of leaf "+leafLocation)
    println("# of branch "+internalLocation/4)
    //InternalHash.foreach(println)
    //LeafHash.foreach(println)

    //println(binnaryopt.getBitString(0,100,internal))
    //println(binnaryopt.getBitString(0,100,leaf))
    dataSBF(maxdatasize, internal, leaf, widthInternal, widthLeaf, InternalHash,LeafHash)


  }

  private def setbitOfBranch(branch: Branch, location: Int, data: Array[Int]) = {

    var beginlocation = location
    branch.nw match {
      case Leaf(_) => binnaryopt.clearBit(beginlocation, data)
      case Branch(_) => binnaryopt.setBit(beginlocation, data)
    }

    beginlocation += 1
    branch.ne match {
      case Leaf(_) => binnaryopt.clearBit(beginlocation, data)
      case Branch(_) => binnaryopt.setBit(beginlocation, data)
    }

    beginlocation += 1
    branch.se match {
      case Leaf(_) => binnaryopt.clearBit(beginlocation, data)
      case Branch(_) => binnaryopt.setBit(beginlocation, data)
    }

    beginlocation += 1
    branch.sw match {
      case Leaf(_) => binnaryopt.clearBit(beginlocation, data)
      case Branch(_) => binnaryopt.setBit(beginlocation, data)
    }

  }

  private def getbitOfBranch(branch: Branch):Int = {

    var count = 0
    branch.nw match {
      case Leaf(_) =>
      case Branch(_) => count+=1
    }

    branch.ne match {
      case Leaf(_) =>
      case Branch(_) => count+=1
    }

    branch.se match {
      case Leaf(_) =>
      case Branch(_) => count+=1
    }

    branch.sw match {
      case Leaf(_) =>
      case Branch(_) => count+=1
    }

    count
  }

  /**
   * println the quadtree structure
   */
  def printTreeStructure(): Unit = {

    //val queue:Queue[Node]=new Queue()

    val queue = new scala.collection.mutable.Queue[Node]

    queue += this.root

    var front = 1
    var end = queue.length
    var depth = 0

    var numofleaf = 0
    var numofbranch = 0

    while (!queue.isEmpty) {

      val pnode = queue.dequeue()

      pnode match {
        case l: Leaf =>
          print(" L(" + l.flag + ") ")
          numofleaf = numofleaf + 1

        case b: Branch =>
          queue.enqueue(b.nw)
          queue.enqueue(b.ne)
          queue.enqueue(b.se)
          queue.enqueue(b.sw)
          printf(" B:(" + b.getbox.toString + ") ")
          numofbranch = numofbranch + 1
      }

      front = front + 1

      if (front > end) {
        depth = depth + 1
        println("\n--------------------------------------------------")
        front = 1
        end = queue.length
      }

    }

    println("depth is " + depth)
    println("number of leaf node is " + numofleaf)
    println("number of branch node is " + numofbranch)

  }

  private def queryBox(node: Node, qspace: Box): Boolean = {

    node match {
      case l: Leaf =>
        if (qspace.intersects(l.getbox)&&l.flag==true)
        {
          l.flag
        }else if(l.getbox.contains(qspace)&&l.flag==false)
        {
          false
        }else
        {
          true
        }

      case b: Branch => {
        b.findChildNodes(qspace).foreach {
          node =>
            if (queryBox(node, qspace) == true) {
              return true
            }
        }
        false
      }
    }

  }

  private def queryBoxWithP(node: Node, qspace: Box, sum: Array[Double]): Unit = {
    node match {
      case l: Leaf =>
        //println("leaf node: "+l.getbox.toBox+" "+l.flag+" ")
        if (qspace.contains(l.getbox) && l.flag == false)
        {
          //println("leaf is:"+l.getbox)
          sum(0) = sum(0) + l.getbox.area
        }

      case b: Branch => {
        b.findChildNodes(qspace).foreach {
          node =>
            queryBoxWithP(node, qspace, sum)
        } //
      }
    }

  }

  /**
   * find child node intersect with the query box
   */
  private def findoneSubbox(branch: Branch, querybox: Box): Iterator[Node] = {

    val iter = new ArrayBuffer[Node]

    if (branch.nw.getbox.contains(querybox)) {
      iter.+=(branch.nw)
    } else if (branch.ne.getbox.contains(querybox)) {
      iter.+=(branch.ne)
    } else if (branch.sw.getbox.contains(querybox)) {
      iter.+=(branch.sw)
    } else if (branch.se.getbox.contains(querybox)) {
      iter.+=(branch.se)
    }

    iter.toIterator
  }

  /**
   * spilt the query box into sub rectangles
   * return order as
   */
  private def spilitQueryBox(input: tmpBox, qnode: Node): Array[tmpBox] = {

    val tmpboxs = new Array[tmpBox](4)

    val midx = (qnode.getbox.x + qnode.getbox.x2) / 2
    val midy = (qnode.getbox.y + qnode.getbox.y2) / 2

    val code = input.alignEdge
    val inputbox = input.inputbox

    //case 3
    if (inputbox.x <= midx && inputbox.y <= midy && inputbox.x2 >= midx && inputbox.y2 >= midy) {
      val a = Box(inputbox.x, midy, midx, inputbox.y2)
      var secode = binnaryopt.clearBit(1, code)
      secode = binnaryopt.clearBit(2, secode)
      tmpboxs(0) = tmpBox(secode, a)

      val b = Box(midx, midy, inputbox.x2, inputbox.y2)
      var bcode = binnaryopt.clearBit(2, code)
      bcode = binnaryopt.clearBit(3, code)
      tmpboxs(1) = tmpBox(bcode, b)

      val c = Box(midx, inputbox.y, inputbox.x2, midy)
      var ccode = binnaryopt.clearBit(0, code)
      ccode = binnaryopt.clearBit(3, ccode)
      tmpboxs(2) = tmpBox(ccode, c)

      val d = Box(inputbox.x, inputbox.y, midx, midy)
      var dcode = binnaryopt.clearBit(0, code)
      dcode = binnaryopt.clearBit(1, code)
      tmpboxs(3) = tmpBox(dcode, d)
      return tmpboxs
    }

    //case 1, for the query box only lay to one subbox
    if (inputbox.x2 < midx && inputbox.y > midy) {
      //nw
      tmpboxs(0) = input
      return tmpboxs
    } else if (inputbox.x > midx && inputbox.y > midy) {
      //ne
      tmpboxs(1) = input
      return tmpboxs
    } else if (inputbox.x >= midx && inputbox.y2 <= midy) {
      //se
      tmpboxs(2) = input
      return tmpboxs
    } else if (inputbox.x2 <= midx && inputbox.y2 <= midy) {
      //sw
      tmpboxs(3) = input
      return tmpboxs
    }


    // case 2: for query box lays to two subboxes
    if (inputbox.x < midx && inputbox.y > midy && inputbox.x2 > midx && inputbox.y2 > midy) {
      //nw and ne
      val nw = Box(inputbox.x, inputbox.y, midx, inputbox.y2)
      val nwcode = binnaryopt.clearBit(1, code)
      tmpboxs(0) = tmpBox(nwcode, nw)

      val ne = Box(midx, inputbox.y, inputbox.x2, inputbox.y2)
      val necode = binnaryopt.clearBit(3, code)
      tmpboxs(1) = tmpBox(necode, ne)
      return tmpboxs

    } else if (inputbox.x >= midx && inputbox.y <= midy && inputbox.x2 > midx && inputbox.y2 > midy) {
      //ne and se

      val ne = Box(inputbox.x, midy, inputbox.x2, inputbox.y2)
      val necode = binnaryopt.clearBit(2, code)
      tmpboxs(1) = tmpBox(necode, ne)

      val se = Box(inputbox.x, inputbox.y, inputbox.x2, midy)
      val secode = binnaryopt.clearBit(0, code)
      tmpboxs(2) = tmpBox(secode, se)

      return tmpboxs

    } else if (inputbox.x < midx && inputbox.y <= midy && inputbox.x2 > midx && inputbox.y2 <= midy) {
      //sw and se

      val se = Box(midx, inputbox.y, inputbox.x2, inputbox.y2)
      val secode = binnaryopt.clearBit(3, code)
      tmpboxs(2) = tmpBox(secode, se)

      val ne = Box(inputbox.x, inputbox.y, midx, inputbox.y2)
      val necode = binnaryopt.clearBit(1, code)
      tmpboxs(3) = tmpBox(necode, ne)
      return tmpboxs

    } else if (inputbox.x < midx && inputbox.y <= midy && inputbox.x2 <= midx && inputbox.y2 > midy) {
      //sw and nw

      val ne = Box(inputbox.x, inputbox.y, inputbox.x2, midy)
      val necode = binnaryopt.clearBit(0, code)
      tmpboxs(3) = tmpBox(necode, ne)

      val se = Box(inputbox.x, midy, inputbox.x2, inputbox.y2)
      val secode = binnaryopt.clearBit(3, code)
      tmpboxs(0) = tmpBox(secode, se)
      return tmpboxs
    }

    tmpboxs
    //case 3: for the query box, lay to four boxes

  }

  /**
   * check weather the current space coalgin any the boundary of the edge
   */
  private def coalignblock(space: tmpBox, qnode: Node): Boolean = {

    //val value=space.alignEdge

    var count = 0

    while (count < 4) {
      //we need to algin on this edge
      if (binnaryopt.getBit(count, space.alignEdge) == 1) {
        //check algin or not
        count match {
          case 0 =>
            if (Math.abs(space.inputbox.y2 - qnode.getbox.y2) > qtreeUtil.errorbound)
              return false
          case 1 =>
            if (Math.abs(space.inputbox.x2 - qnode.getbox.x2) > qtreeUtil.errorbound)
              return false
          case 2 =>
            if (Math.abs(space.inputbox.y - qnode.getbox.y) > qtreeUtil.errorbound)
              return false
          case 3 =>
            if (Math.abs(space.inputbox.x - qnode.getbox.x) > qtreeUtil.errorbound)
              return false
        }
      }

      count = count + 1
    }

    true
  }

  /**
   * mark this node's sub children as false
   * @param node
   * @param box
   */
  private def markleafNode(node: Node, box: Box): Boolean = {

    if (!box.contains(node.getbox)) {
      return false
    }

    node match {
      case l: Leaf =>
        l.flag = false
        true
      case b: Branch =>
        markleafNode(b.ne, box)
        markleafNode(b.nw, box)
        markleafNode(b.se, box)
        markleafNode(b.sw, box)
        true
    }

  }

  /**
   * return the new parent
   * @param old
   * @param branch
   * @return
   */
  private def relinkPointer(old: Node, branch: Node) = {

    old.parent match {
      case parent: Branch =>
        //set up the pointer for this new node
        if (old.equals(parent.ne)) {
          parent.ne = branch
        } else if (old.equals(parent.nw)) {
          parent.nw = branch

        } else if (old.equals(parent.se)) {
          parent.se = branch
        } else if (old.equals(parent.sw)) {
          parent.sw = branch
        }

        branch.parent = parent

    }

  }

  /**
   * insert the box into quadtree, and mark the leaf node as false
   */
  private def insertBox(parent: Node, space: tmpBox): Unit = {

    if (!parent.getbox.contains(space.inputbox)) {
      return
    }

    parent match {
      case l: Leaf =>

        if (l.flag == false && l.getbox.contains(space.inputbox)) {
          return
        }

        //check which edge of leaf coalgin with the current box
        if (coalignblock(space, l) || space.inputbox.contains(l.getbox)) {
          //if all the required edge is matched or the input box contain the leaf node return
          l.flag = false
          return
        } else {
          //spilit the current leaf node and replace with an internal node
          //add condition to spilit the leaf node
          //condition 1: the leaf node is big enough, when it compare to input box
          if (qtreeUtil.getAreaRatio(l.getbox, this.currentbox) > qtreeUtil.leafStopBound) {

            //println("spilit this leaf node: " + l.getbox.toString)
            val branch = l.spilitLeafNode
            relinkPointer(l, branch)
            // spilit the current query box
            val spilitbox = spilitQueryBox(space, l)

            spilitbox.foreach {
              newnode => newnode match {
                case null =>
                case _ =>
                  findoneSubbox(branch, newnode.inputbox).foreach(
                    overlapnode => insertBox(overlapnode, newnode)
                  )
              }
            }
          } else {
            //do not spilit this bound
            //println("stop at this leaf: "+l.getbox.toString)
            //println("current box is: "+this.currentbox.toString)
            //println("ratio=== "+qtreeUtil.getAreaRatio(l.getbox, this.currentbox))
          }

        } //else

      case b: Branch => {
        //println("go through this branch node: " + b.getbox.toString)

        var indicator = false
        b.findChildNodes(space.inputbox).foreach {
          children =>
            if (space.inputbox.contains(children.getbox) || coalignblock(space, children)) {
              //mark all his children leaf node as false
              //markleafNode(children,space.inputbox)
              children match {
                case lchildren: Leaf =>
                  lchildren.flag = false

                //ascend the branch node into leaf node
                case bchildren: Branch =>
                  val newleaf = Leaf(bchildren.getbox)
                  newleaf.flag = false
                  relinkPointer(bchildren, newleaf)
              }

            } else {
              indicator = true
            }
        }

        //we need to go for subbranch searching
        if (indicator) {
          val spilitbox = spilitQueryBox(space, b)

          spilitbox.foreach {
            newnode => newnode match {
              case null =>
              case _ =>
                findoneSubbox(b, newnode.inputbox).foreach {
                  overlapnode => {
                    //println("overlap node "+overlapnode.getbox.toString)
                    insertBox(overlapnode, newnode)
                  }
                }
            }
          } //for each box

        } //if indicator
      } //case for branch
    }

  }

  /**
   * alignEdge=1011 is the binary code to require which edge to algin with quadtree block
   * the order is North, east, south, west,
   */
  case class tmpBox(alignEdge: Int, inputbox: Box)


}

object QTree {


  /**
   * build a QTree from a query box
   * @param querybox
   */
  def apply(querybox: Box): QTree = {
    val qtree = QTree.empty
    qtree.insertBox(querybox)
    qtree
  }

  /**
   * Construct an QTree from a sequence of entries.
   */
  def apply(itr: Iterator[Box]): QTree = {
    val qtree = QTree.empty
    itr.foreach { box => qtree.insertBox(box) }
    qtree
  }

  /**
   * Construct an empty QTree.
   */
  def empty: QTree = {
    val leaf = Leaf(qtreeUtil.wholespace)
    val branch = leaf.spilitLeafNode
    new QTree(branch)
  }

}
