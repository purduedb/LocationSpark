package cs.purdue.edu.spatialindex.quatree

import cs.purdue.edu.spatialbloomfilter.{dataSBFV2, binnaryopt, dataSBF, qtreeUtil}
import cs.purdue.edu.spatialindex.rtree.{Geom, Box}

import scala.collection.mutable
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

case class SBQTree() extends serializable{

  var budget = 0
  var root: Node = null
  var numofBranch=0
  var numofLeaf=0
  var currentbox: Box = null

  val lrucache=new LRU(100)

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
   * train the SBFilter from the data,this process work as inserting points into quadtree, but i never record
   * the data itself, it only mark the leaf node with data as leaf, and other leaf as false
   * @param itr
   */
  def trainSBfilter(itr: Iterator[Geom])=
  {
      itr.foreach{p=>this.insertPoint(p)}
      //this.getSBFilter()
  }

  /**
   * this function is used for training the SBFilter,
   * it never store the data inside the leaf node
   * @param point
   */
  protected def insertPoint(point:Geom): Unit =
  {
    insertPoint(this.root,point)
  }

  /**
   * this function is used for training the SBQtree
   * @param point
   */
  protected def insertPoint(parent: Node, point:Geom): Unit =
  {
    if (!parent.getbox.contains(point)) {
      return
    }

    parent match {
      case l: Leaf => {
        if (l.count < qtreeUtil.leafbound) {
          l.flag = true
          l.count+=1
          return

        } else {
          //split this leaf node
          val branch = l.spilitLeafNode
          this.numofBranch += 1
          this.numofLeaf += 3
          relinkPointer(l, branch)

          //mark branch's leaf node as false
          markChildrenAsFalse(branch)

          branch.findChildNodes(point).foreach {
            children =>
              insertPoint(children, point)
          }

        }
      }

      case b: Branch => {
        //println("go through this branch node: " + b.getbox.toString)
        b.findChildNodes(point).foreach {
          children=>
            insertPoint(children,point)
        }

      }

    }

  }

  private def markChildrenAsFalse(branch:Branch)={

    branch.nw match {
      case l:Leaf => l.flag=false
    }

    branch.ne match {
      case l:Leaf => l.flag=false
    }

    branch.sw match {
      case l:Leaf => l.flag=false
    }

    branch.se match {
      case l:Leaf => l.flag=false
    }

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
   * insert the box without any data into quadtree, and mark the leaf node as false
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

            val branch = l.spilitLeafNode
            this.numofBranch+=1
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
                  this.numofBranch-=1
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
    //println((sum(0)))
   // println(qspace.area)
    (sum(0) / qspace.area)

  }

  /**
   * insert a box if the true postive happen
   * @param space
   */
  def insertTrueBox(space: Box) = {

  }


  /**
   * get the binary code of the quadtree
   */
  def getSBFilter(): dataSBF = {

    val maxdatasize = this.numofBranch/4

    //define the global int array
    val internal = new Array[Int](this.numofBranch/4) //where 10 is the default size
    val leaf = new Array[Int](this.numofBranch/4) //where 10 is the default size

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

    var falseleaf=0
    //this is used for the bound number

    while (!queue.isEmpty) {

      val pnode = queue.dequeue()

      pnode match {
        case l: Leaf => {

          if (l.flag) {
            binnaryopt.setBit(leafLocation, leaf)
          }
          else {
            binnaryopt.clearBit(leafLocation, leaf)
            falseleaf+=1
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
          internalLocation += 4
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

    //println("# of leaf "+leafLocation)
    //println("# of false leaf "+falseleaf)
   // println("# of branch "+internalLocation/4)
    //println("# of candidate to merge: "+this.lrucache.getNumnode())
    //println("leaf node: "+binnaryopt.getBitString(0,200,leaf))
    dataSBF(maxdatasize, internal, leaf, widthInternal, widthLeaf)

  }

  /**
   * get the binary code of the quadtree
   * this version to store the location of the children, but this version is 5 times slow than the v1
   */
  def getSBFilterV2(): dataSBFV2 = {

    val maxdatasize = this.numofBranch/4

    //define the global int array
    val internal = new Array[Int](maxdatasize) //where 10 is the default size
    val leaf = new Array[Int](maxdatasize) //where 10 is the default size

    //var widthInternal = ArrayBuffer[Int]() //number of internal node for that depth
   // var widthLeaf = ArrayBuffer[Int]() //number of leaf nodes

    val queue = new scala.collection.mutable.Queue[Node]

    queue += this.root

    var numberLeaf = 0
    var front = 1
    var end = queue.length
    var depth = 1
    var leafLocation = 0
    var internalLocation = 0

    //this is used for the bound number
    var numOnesToBound=1
    var numZerosToBound=0

    val InternalHash = scala.collection.mutable.HashMap.empty[Int,Int]
    val LeafHash = scala.collection.mutable.HashMap.empty[Int,Int]
    var falseleaf=0

    while (!queue.isEmpty) {

      val pnode = queue.dequeue()

      pnode match {
        case l: Leaf => {

          if (l.flag) {
            binnaryopt.setBit(leafLocation, leaf)
          }
          else {
            binnaryopt.clearBit(leafLocation, leaf)
            falseleaf+=1
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

          {
            InternalHash.+=(internalLocation->numOnesToBound*qtreeUtil.binnaryUnit)
            LeafHash.+=(internalLocation->numZerosToBound)
          }

          numOnesToBound+=getbitOfBranch(b)
          numZerosToBound+=4-getbitOfBranch(b)

          internalLocation += 4

        //println("branch: "+b.getbox.toString)
        //println("internal: "+internalLocation)
        // println(binnaryopt.getBitString(0,100,internal))

      }

      front = front + 1

      if (front > end) {
        //widthLeaf+=(numberLeaf)
        //widthInternal+=(end - numberLeaf)
        depth += 1
        front= 1
        end = queue.size
        numberLeaf = 0
        //
      }

    }

    //println("# of leaf "+leafLocation)
   // println("# of false leaf "+falseleaf)
    //println("# of branch "+internalLocation/4)
   // InternalHash.foreach(println)

    dataSBFV2(maxdatasize, depth ,internal, leaf, InternalHash,LeafHash)

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

        if (qspace.intersects(l.getbox))
        {
          //println("intersect leaf is: "+l.getbox+" l.label "+l.flag)

          if(l.flag==true)
          {
            return true
          }
          else if(l.getbox.contains(qspace)&&l.flag==false)
          {
            false
          }
          //we need to procede
          false

        }else
        {
          false
        }

      case b: Branch => {

        //println("intersect branch is:"+b.getbox)

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
        //println("*"*50)

        if (qspace.contains(l.getbox) && l.flag == false)
        {
          //println("contain leaf is:"+l.getbox)
          //println("contain leaf area is: "+  l.getbox.area)
          sum(0) = sum(0) + l.getbox.area
        }else if (!qspace.contains(l.getbox)&&qspace.intersects(l.getbox) && l.flag == false)
        {
         // println("l leaf is "+ l.getbox)
         // println("intersect area is: "+ qspace.intersectionarea(l.getbox))
          sum(0) = sum(0) + qspace.intersectionarea(l.getbox)
        }

      case b: Branch => {

          //println("intersect branch is"+ b.getbox)

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
  protected def findoneSubbox(branch: Branch, querybox: Box): Iterator[Node] = {

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
  protected def spilitQueryBox(input: tmpBox, qnode: Node): Array[tmpBox] = {

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
  protected def coalignblock(space: tmpBox, qnode: Node): Boolean = {

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
   * return the new parent
   */
  protected def relinkPointer(old: Node, branch: Node) = {

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
   * LRU based approach to merge node with four leaf node
   * (1) this LRU is based on the hash table and linkedlist
   * (2) once a leaf node is used, update his parent's location in the LRU and move to the tail
   * (3) once the budget is used up, remove it from the lru, and make this head of linkedist as leaf
   */
  def mergeNodes(budget:Int) = {

    while(this.numofLeaf>budget&&this.lrucache.getNumnode()>0)
    {
      this.lrucache.popHead() match
      {
        //change this node to leaf node
        //mark this new node based on his children's flag
        case b:Branch=>
          val newleaf = Leaf(b.getbox)
          newleaf.flag =b.findChildFlag()
          relinkPointer(b, newleaf)
          this.numofLeaf-=3
          this.numofBranch-=1
      }
    }

  }


  //val queue:Queue[Node]=new Queue()

  protected  def allchildrenisLeaf(b:Branch):Boolean={

    if(b.nw.isInstanceOf[Leaf]&&b.ne.isInstanceOf[Leaf]&&b.sw.isInstanceOf[Leaf]&&b.se.isInstanceOf[Leaf])
    {
      (b.nw.asInstanceOf[Leaf].flag&&b.ne.asInstanceOf[Leaf].flag&&b.sw.asInstanceOf[Leaf].flag&&b.se.asInstanceOf[Leaf].flag)
    }else
    {
      false
    }

  }

  /**
   * for one branch with four children is leaf and all their flag is true
   * @return
   */
  def mergeBranchWIthAllTrueLeafNode() :Int=
  {
    val queue = new scala.collection.mutable.Queue[Node]
    queue += this.root

    var front = 1
    var end = queue.length
    var depth = 0

    var numAlltrue=0
    while (!queue.isEmpty) {

      val pnode = queue.dequeue()

      pnode match {
        case l: Leaf =>
          //print(" L(" + l.flag + ") ")
         //numofleaf = numofleaf + 1

        case b: Branch =>
          queue.enqueue(b.nw)
          queue.enqueue(b.ne)
          queue.enqueue(b.se)
          queue.enqueue(b.sw)

          if(allchildrenisLeaf(b))
          {
            numAlltrue+=1
            val newleaf = Leaf(b.getbox)
            newleaf.flag =b.findChildFlag()
            relinkPointer(b, newleaf)
            this.numofLeaf-=3
            this.numofBranch-=1
          }
      }

      front = front + 1

      if (front > end) {
        depth = depth + 1
        //println("\n--------------------------------------------------")
        front = 1
        end = queue.length
      }

    }

    numAlltrue

  }



  /**
   * alignEdge=1011 is the binary code to require which edge to algin with quadtree block
   * the order is North, east, south, west,
   */
  case class tmpBox(alignEdge: Int, inputbox: Box)


}

object SBQTree {

  /**
   * build a QTree from a query box
   * @param querybox
   */
  def apply(querybox: Box): SBQTree = {
    val qtree = SBQTree.empty
    qtree.insertBox(querybox)
    qtree
  }

  /**
   * Construct an QTree from a sequence of entries.
   */
  def apply(itr: Iterator[Box]): SBQTree = {
    val qtree = SBQTree.empty
    itr.foreach { box => qtree.insertBox(box) }
    qtree
  }

  /**
   * Construct an empty QTree.
   */
  def empty: SBQTree = {
    val leaf = Leaf(qtreeUtil.wholespace)
    val branch = leaf.spilitLeafNode
    new SBQTree(branch)
  }

}

/**
 * this class is used for adptive merge nodes for query pattern
 * (1) LRU based approach is implemented, but it very ineffecient
 * (2) propose the batch and clock based approach
 * (3)
 */
class QTreeCache() extends SBQTree
{

  def this(size: Int) {
    this
    budget = size
    this.root = Leaf(qtreeUtil.wholespace).spilitLeafNode
  }
  /**
   * insert a box and record the internal node to merge
   * this is 50 times slow the original approach
   * @param space
   */
  def insertBoxWithCache(space: Box): Unit = {

    currentbox = space
    val querybox = tmpBox(15, space)

    insertBoxWithCache(this.root, querybox)
  }

  /**
   * insert the box without any data into quadtree, and mark the leaf node as false
   * if all four children is leaf node, return ture, else return true
   */
  private def insertBoxWithCache(parent: Node, space: tmpBox): Boolean = {

    if (!parent.getbox.contains(space.inputbox)) {
      return true
    }

    parent match {
      case l: Leaf =>

        if (l.flag == false && l.getbox.contains(space.inputbox)) {
          return true
        }

        //check which edge of leaf coalgin with the current box
        if (coalignblock(space, l) || space.inputbox.contains(l.getbox)) {
          //if all the required edge is matched or the input box contain the leaf node return
          l.flag = false
          return true
        } else {
          //spilit the current leaf node and replace with an internal node
          //add condition to spilit the leaf node
          //condition 1: the leaf node is big enough, when it compare to input box
          if (qtreeUtil.getAreaRatio(l.getbox, this.currentbox) > qtreeUtil.leafStopBound) {

            val branch = l.spilitLeafNode
            this.numofBranch+=1
            this.numofLeaf+=3

            relinkPointer(l, branch)
            //remove this node from the candidate

            //var b2=System.currentTimeMillis
            lrucache.removeNode(l.parent)
            //println("remove node time: "+(System.currentTimeMillis-b2) +" ms")

            // spilit the current query box
            val spilitbox = spilitQueryBox(space, l)

            var allleaf=true

            spilitbox.foreach {
              newnode => newnode match {
                case null =>
                case _ =>
                  findoneSubbox(branch, newnode.inputbox).foreach {
                    overlapnode =>
                      allleaf=allleaf&&insertBoxWithCache(overlapnode, newnode)
                  }
              }
            }

            //if whole children node is leaf node, put this node into the LRU
            if(allleaf)
            {
              lrucache.setNode(branch)
            }

            allleaf
          } else {
            //do not spilit this bound
            true
          }

        } //else

      case b: Branch => {
        //println("go through this branch node: " + b.getbox.toString)

        var indicator = false
        b.findChildNodes(space.inputbox).foreach {
          children =>
            //first merge strategy
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
                  this.numofLeaf-=3
                  this.numofBranch-=1
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
                var allleaf=true
                findoneSubbox(b, newnode.inputbox).foreach {
                  overlapnode => {
                    //println("overlap node "+overlapnode.getbox.toString)
                    allleaf=allleaf&&insertBoxWithCache(overlapnode, newnode)
                  }
                }

                if(allleaf==true)
                {
                  //this branch with four leaf node
                  lrucache.setNode(b)
                }
            }
          } //for each box

        } //if indicator

        false
      } //case for branch
    }

  }

  /**
   * this way to query the index, need to mark record the query frequency in the LRU cache
   * @param qspace
   * @return
   */
  def queryBoxWithCache(qspace:Box):Boolean={

    queryBoxWithCache(this.root, qspace)

  }

  /**
   *if one leaf node is used, we put its parent into the tail of queue
   */
  private def queryBoxWithCache(node: Node, qspace: Box): Boolean = {

    node match {
      case l: Leaf =>
        if (qspace.intersects(l.getbox)&&l.flag==true)
        {
          lrucache.putNodeTotail(l.parent)
          l.flag
        }else if(l.getbox.contains(qspace)&&l.flag==false)
        {
          //if this node is used, we put its parent into the tail of cache
          lrucache.putNodeTotail(l.parent)
          false
        }else
        {
          true
        }

      case b: Branch => {
        b.findChildNodes(qspace).foreach {
          node =>
            if (queryBoxWithCache(node, qspace) == true) {
              return true
            }
        }
        false
      }
    }

  }

}


case class LRU()
{
  //val list=mutable.DoubleLinkedList.empty[Node]

  val list=mutable.ListBuffer.empty[Node]

  var cachmaxsize=0

  def this(maxsize:Int)
  {
    this
    cachmaxsize=maxsize
  }

  //private case class lnode(value:Node,next:Node)
  val hash=mutable.HashMap.empty[Node,Node]


  def getNumnode():Int={
    this.list.size
  }

  def popHead():Node={

    val ret=list.head
    hash.remove(ret)
    list.drop(0)
    ret
  }

  /**
   * put this node into the tail of list
   * @param update
   */
  def setNode(update:Node)=
  {

    //var b2=System.currentTimeMillis

    if(hash.contains(update))
    {
      list.-=(update)
      list.append(update)
      //list.insert(list.length,update)
    }
    else
    {
      hash.put(update,update)
      list.append(update)
    }

   // println("set node time: "+(System.currentTimeMillis-b2) +" ms")

  }

  def appendNode(update:Node)={

    if(hash.contains(update))
    {
      //list.insert(list.length,update)
    }
    else
    {
      hash.put(update,update)
      list.append(update)
    }
  }

  /**
   * put this node into the tail of list
   * @param update
   */
  def putNodeTotail(update:Node)=
  {

   //var b2=System.currentTimeMillis

    if(hash.contains(update))
    {
      list.-=(update)
      list.append(update)
      //list.insert(list.length,update)
    }

    //println("move node to tail time: "+(System.currentTimeMillis-b2) +" ms")

  }

  def removeNode(update:Node)={

    if(hash.contains(update))
    {
      list.-=(update)
      //list.insert(list.length,update)
    }
  }

}


