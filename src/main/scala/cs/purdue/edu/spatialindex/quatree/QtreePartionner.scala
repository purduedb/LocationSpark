package cs.purdue.edu.spatialindex.quatree

import cs.purdue.edu.spatialbloomfilter.qtreeUtil
import cs.purdue.edu.spatialindex.rtree.{Box, Geom}

import scala.collection.mutable
import scala.collection.mutable.{HashSet, ArrayBuffer}

/**
 * Created by merlin on 11/18/15.
 */
class QtreeForPartion() extends Serializable{

  var root: Node = null

  var leafbound=0

  def this(leafbound:Int) {
    this
    this.leafbound=leafbound
    this.root = new leafwithcount(qtreeUtil.wholespace).spilitLeafNode
  }


  /**
   * this function is used for training the SBFilter,
   * it never store the data inside the leaf node
   * @param point
   */
   def insertPoint(point:Geom): Unit =
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
      case l: leafwithcount => {
        if (l.count < this.leafbound) {
          l.count+=1
          l.updatecount(point)
          return

        } else {
          //split this leaf node
          val branch = l.spilitLeafNode
          relinkPointer(l, branch)
          val subbranch=this.findSubchildren(branch,point)
          insertPoint(subbranch,point)
        }
      }

      case b: Branch => {
        //println("go through this branch node: " + b.getbox.toString)
        val branch=this.findSubchildren(b,point)
        insertPoint(branch,point)
      }
    }

  }

  /**
   * return the new parent
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
   * annotate those leaf node with pid
   */
  def computePIDofLeaf(total:Int,numPartition:Int):Int={

    //val queue:Queue[Node]=new Queue()

    val queue = new scala.collection.mutable.Queue[Node]

    queue += this.root

    var front = 1
    var end = queue.length
    var depth = 0
    var pid=0

    val tmp=new ArrayBuffer[leafwithcount]()

    val bound=total/numPartition
    var countindex=0

    while (!queue.isEmpty) {

      val pnode = queue.dequeue()

      pnode match {

        case l: leafwithcount =>

          if((l.count+countindex)<bound)
          {

            countindex=countindex+l.count
          }else
          {
            tmp.foreach(l=>l.id=pid)
            pid+=1
            countindex=0
            tmp.clear()
          }

          tmp.+=(l)

        case b: Branch =>
          queue.enqueue(b.nw)
          queue.enqueue(b.ne)
          queue.enqueue(b.se)
          queue.enqueue(b.sw)

      }

      front = front + 1

      if (front > end) {
        depth = depth + 1
        front = 1
        end = queue.length
      }

    }//for bfs

    tmp.foreach(l=>l.id=pid)

    pid+1

  }

  /**
   * get the overlap partition region for the input box
   * @param box
   * @return
   */
  def getPIDforBox(box:Box):HashSet[Int]=
  {
    val ret=new mutable.HashSet[Int]()

    getPIDforBox(box,ret,this.root)

    ret
  }

  /**
   * recursive to find the overlap leaf node for the input box
   * @param box
   * @param ret
   * @param n
   */
  private def getPIDforBox(box:Box,ret:HashSet[Int], n:Node):Unit=
  {
    n match {
      case l: leafwithcount =>

        // println("search leaf "+l.getbox)
        if (l.getbox.intersects(box))
        {
          ret.+=(l.id)
        }

      case b: Branch => {
        //println("brach is "+b.getbox)
        b.findChildNodes(box).foreach(child=>getPIDforBox(box,ret,child))
      }
    }

  }

  /**
   * find which leaf node this leaf belong to
   * @param p
   * @return
   */
  def getPID(p:Geom):Int={

      //println("get pid")

     getPID(this.root,p)

  }

  private def getPID(n:Node,p:Geom):Int={

    n match {
      case l: leafwithcount =>

       // println("search leaf "+l.getbox)

        if (l.getbox.contains(p))
        {
           l.id
        }else
        {
          0
        }

      case b: Branch => {

        //println("brach is "+b.getbox)

       val child=findSubchildren(b,p)

        if(child!=null)
        {
          getPID(child,p)
        }else
        {
          0
        }

      }
    }

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
        case l: leafwithcount =>
          print(" L(" + l.id + ") ")
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

  private def findSubchildren(b:Branch,p:Geom):Node={

    if(b.nw.getbox.contains(p))
    {
      return b.nw
    }

    if(b.ne.getbox.contains(p))
    {
      return b.ne
    }

    if(b.sw.getbox.contains(p))
    {
      return b.sw

    }

    if(b.se.getbox.contains(p))
    {
      return b.se
    }

    null
  }


}
