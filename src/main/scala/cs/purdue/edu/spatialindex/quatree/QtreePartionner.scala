package cs.purdue.edu.spatialindex.quatree

import cs.purdue.edu.spatialindex.rtree.{Point, Box, Geom}
import cs.purdue.edu.spatialindex.spatialbloomfilter.qtreeUtil

import scala.collection.mutable
import scala.collection.mutable.{HashSet, ArrayBuffer}

/**
 * Created by merlin on 11/18/15.
 */
class QtreeForPartion() extends Serializable{

  var root: Node = null
  var depth=0

  var leafbound=10

  var maxpartitionid=0

  def this(leafbound:Int) {
    this
    this.leafbound=leafbound
    this.root = new leafwithcount(qtreeUtil.wholespace).spilitLeafNode
  }

  /**
   * colone this quadtree's data structure
   * @return
   */
  def coloneTree():Node=
  {
     def coloneTree(node:Node):Node=
    {
      node match {
        case l: leafwithcount =>
          //copy this leaf node
          val newl=new leafwithcount(l.getbox)
          newl.id=l.id
          newl.count=l.count
          newl

        case b: Branch => {

          val newbranch= Branch(b.space)
          newbranch.ne=coloneTree(b.ne)
          newbranch.nw=coloneTree(b.nw)
          newbranch.se=coloneTree(b.se)
          newbranch.sw=coloneTree(b.sw)

          newbranch
        }
      }
    }

    coloneTree(this.root)
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
          if(subbranch==null)
            return
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
   * compute the frequency of the box to visit the leaf node
   * @param box
   */
  def visitleafForBox(box:Box):Unit=
  {
    visitleafForBox(box,this.root)
  }

  /**
   *
   * @param box
   * @param node
   */
  private def visitleafForBox(box:Box, node:Node):Unit=
  {

    if (!node.getbox.intersects(box)) {
      return
    }

    node match {
      case l: leafwithcount =>

        if (l.getbox.intersects(box))
        {
          l.visitcount+=1
        }

      case b: Branch => {
        //println("brach is "+b.getbox)
        b.findChildNodes(box).foreach(child=>visitleafForBox(box,child))
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
    //this.printTreeStructure()

    val queue = new scala.collection.mutable.Queue[Node]

    queue += this.root

    var pid=0

    val tmp=new ArrayBuffer[leafwithcount]()

    val bound=total/numPartition

    var countindex=0
    var leafcount=0

    while (!queue.isEmpty) {

      val pnode = queue.dequeue()

      pnode match {

        case l: leafwithcount =>

          if((l.count+countindex)<bound)
          {
            countindex=countindex+l.count
            leafcount+=1
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

    }//for bfs

    tmp.foreach(l=>l.id=pid)

    this.maxpartitionid=pid+1

    pid+1

  }

  /**
   * annotate those leaf node with pid
   */
  def computePIDofLeafDFS(total:Int,numPartition:Int):Int={

    //val queue:Queue[Node]=new Queue()

    val stack = new scala.collection.mutable.Stack[Node]

    stack.push(this.root)
    //queue +=

    var pid=0

    val tmp=new ArrayBuffer[leafwithcount]()

    val bound=total/numPartition
    var countindex=0

    while (!stack.isEmpty) {

      val pnode = stack.pop()

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
          stack.push(b.nw)
          stack.push(b.ne)
          stack.push(b.se)
          stack.push(b.sw)

      }

    }//for bfs

    tmp.foreach(l=>l.id=pid)

    pid+1

  }
  /**
   * annotate those leaf node based on the query distribution
   */
  def computePIDBasedQueries(map:Map[Int,Int]):Int={

    def averagepartition(stat:ArrayBuffer[leafwithcount],startpid:Int, partitionnumber:Int):Int={

      //randomnize this array list
      val list=util.Random.shuffle(stat)

      var eachpartitionnumber=list.size/partitionnumber

      if(eachpartitionnumber==0||eachpartitionnumber<=3)
         eachpartitionnumber=5

      var pid=startpid

      if(list.size>1)
      {
        list(0).id=pid
        for(i <-1 to list.size-1)
        {
          if(i%eachpartitionnumber==0)
            pid+=1
          list(i).id=pid
        }
        pid+1
      }else if(list.size==1)
      {
        list(0).id=pid
        pid
      }else
      {
        pid
      }

    }

    /**
     *each partition have the similar number of visit count
     */
    def averagesum(stat:ArrayBuffer[leafwithcount],startpid:Int, total:Int, partitionnumber:Int):Int={

      val list=stat.sortBy(l=>(l.visitcount+1)*l.count)(Ordering[Int].reverse)

      val tmp=new ArrayBuffer[leafwithcount]
      var pid=startpid
      var tmpsum=0
      val target=total/partitionnumber
      var begin=0
      var end=list.size-1

      while(begin<=end)
      {
         //tmp.+=(list(end))
         if(tmpsum<target)
         {
            if(Math.abs(target-tmpsum-(list(begin).visitcount+1)*list(begin).count)>Math.abs(target-tmpsum-(list(end).visitcount+1)*list(end).count))
            {
               tmpsum=tmpsum+(list(end).visitcount+1)*list(end).count
               tmp.+=(list(end))
               end=end-1
            }else
            {
               tmp.+=(list(begin))
               tmpsum=tmpsum+(list(begin).visitcount+1)*list(begin).count
               begin=begin+1
            }

         }else
         {
            tmp.foreach(e=>e.id=pid)
            pid+=1
            tmp.clear()
            tmpsum=0
         }
      }

      if(tmp.size!=0)
      {
        pid=averagepartition(tmp,pid,partitionnumber-(pid-startpid))
      }

      pid
    }

    def recomputePidForSkew(list:ArrayBuffer[leafwithcount], startpid:Int, partitionnumber:Int):Int=
    {
      var total=0
      list.foreach(l=> total=total+((l.visitcount+1)*l.count))
      val threshold=total/partitionnumber
      if(total==0||threshold==0) //in case the sample is not precise
      {
        averagepartition(list,startpid,partitionnumber)
      }else // we can find those partition
      {
        averagesum(list,startpid,total,partitionnumber)
      }
    }

    val queue = new scala.collection.mutable.Queue[Node]

    queue += this.root
    val tmp=new ArrayBuffer[leafwithcount]()
    val nonskew=new ArrayBuffer[leafwithcount]()

    var currentpid=0
    var startpid=0

    while (!queue.isEmpty) {

      val pnode = queue.dequeue()
      pnode match {
        case l: leafwithcount =>

          if(map.contains(l.id))
          {
            if(tmp.length==0)
            {
              currentpid=l.id
              tmp.+=(l)
            }else
            {
              if(currentpid!=l.id)
              {
                startpid=recomputePidForSkew(tmp,startpid,map.get(currentpid).get)
                tmp.clear()
                currentpid=l.id
                tmp.+=(l)
              }else
              {
                tmp.+=(l)
              }
            }

          }else
          {
            if(tmp.size!=0)
            {
              startpid=recomputePidForSkew(tmp,startpid,map.get(currentpid).get)
              tmp.clear()
            }
            currentpid=l.id
            nonskew.+=(l)
          }

        case b: Branch =>
          queue.enqueue(b.nw)
          queue.enqueue(b.ne)
          queue.enqueue(b.se)
          queue.enqueue(b.sw)
      }

    }//for bfs

    if(tmp.size!=0)
    {
      val partitionnumber=map.get(tmp(0).id).getOrElse(5)
      startpid=recomputePidForSkew(tmp,startpid,partitionnumber)
      tmp.clear()
    }

    //in order to reduce the possiblity of the sampling approach is precise
    if(nonskew.size>50)
    {
      startpid=averagepartition(nonskew,startpid+1,10)
    }
    else
    {
      nonskew.foreach(e=>e.id=startpid+1)
    }

    startpid+1
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
   * for the input box, find the corresponding point in each partition.
    * @param box
   * @return
   */
  def getPointForRangeQuery(box:Box):HashSet[Point]=
  {
    val ret=new mutable.HashSet[Point]()

    getPointForRangeQuery(box,ret,this.root)

    ret
  }
  /**
   * recursive to find the overlap leaf node for the input box
   * @param box
   * @param ret
   * @param n
   */
  private def getPointForRangeQuery(box:Box,ret:HashSet[Point], n:Node):Unit=
  {
    n match {
      case l: leafwithcount =>

        //println("search leaf "+l.getbox)
        if (l.getbox.intersects(box))
        {
          //ret.+=(l.id)
          ret.+=(Point((l.getbox.x+l.getbox.x2)/2,(l.getbox.y+l.getbox.y2)/2))
        }

      case b: Branch => {
        //println("brach is "+b.getbox)
        b.findChildNodes(box).foreach(child=>getPointForRangeQuery(box,ret,child))
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
          throw new IllegalStateException("data point is not correct location format")
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
          throw new IllegalStateException("data point is not correct location format")
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
          print(" L(id: " + l.id+" vc:"+l.visitcount +" count:" +l.count+") ")
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
