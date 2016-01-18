package cs.purdue.edu.spatialindex.quatree

import cs.purdue.edu.spatialindex.rtree.{Geom, Box, Entry, Point}
import cs.purdue.edu.spatialindex.spatialbloomfilter.qtreeUtil
import scala.collection.mutable.{ArrayBuffer, PriorityQueue}

/**
 * Created by merlin on 11/23/15.
 */
case class QTree[V] (LEAF_MAX_CAPACITY: Int,space:Box){

  var root: Node = new leafwithstorage(space)
  var depth=0
  var size=0

  /**
   * Insert a value into the tree at (x, y), returning a new tree.
   */
  def insert(x: Float, y: Float, value: V):Unit =
  {
    insert(Entry(Point(x, y), value))
  }

  /**
   * Insert a value into the tree at (x, y), returning a new tree.
   */
  def insert(x:Point, value: V):Unit=
  {
    insert(Entry(x, value))
  }

  /**
   * Insert an entry into the tree, returning a new tree.
   */
  def insert(entry: Entry[V]):Unit= {

      this.insert(this.root,entry)
  }

  /**
   * Insert an entry into the tree, and users can define the function for handling the
   * duplicate keys, for example, one location can have the same keys, but different text information and time
   */
  def insert(entry: Entry[V], f:(V,V)=>V):Unit= {

    def rucr(node:Node, entry:Entry[V],f:(V,V)=>V):Unit= {

      if(!this.root.getbox.contains(entry.geom))
      {
        return
      }

      node match
      {
        case l: leafwithstorage[V]=>
          if(l.count<this.LEAF_MAX_CAPACITY)
          {
              if(l.storage.contains(entry.geom))
              {
                val oldv=l.storage.get(entry.geom).get
                val newv=f(entry.value,oldv)
                l.storage.put(entry.geom,newv)
              }else
              {
                l.addEntry(entry)
                //size+=1
              }
            size+=1
          }else
          {
            val subbranch=l.spilitLeafNode
            this.size-=l.storage.size
            l.storage.foreach{
              case(k,v)=>
                this.insert(subbranch,Entry[V](k.asInstanceOf[Geom],v))
            }
            l.clean
            relinkPointer(l,subbranch)
            rucr(subbranch,entry,f)
          }
        case b: Branch=>
          val subbranch=this.findSubchildren(b,entry.geom)
          rucr(subbranch,entry,f)
      }

    }

    rucr(this.root,entry,f)
  }

  /**
   * Insert an entry into the tree, returning a new tree.
   */
 private def insert(node:Node, entry: Entry[V]):Unit= {

    if(!node.getbox.contains(entry.geom))
    {
      return
    }

    node match
    {
      case l: leafwithstorage[V]=>
        if(l.count<this.LEAF_MAX_CAPACITY)
          {
              if(l.addEntry(entry))
              {
                size+=1
              }

          }else
          {
            val subbranch=l.spilitLeafNode
            this.size-=l.storage.size
            l.storage.foreach{
              case(k,v)=>
                this.insert(subbranch,Entry[V](k.asInstanceOf[Geom],v))
            }
            l.clean
            relinkPointer(l,subbranch)
            this.insert(subbranch,entry)
          }
      case b: Branch=>
        val subbranch=this.findSubchildren(b,entry.geom)
        insert(subbranch,entry)
    }

  }

  /**
   * return the new parent
   */
  protected def relinkPointer(old: Node, branch: Node):Unit = {

    if(old.parent==null&&(!old.equals(this.root)))
    {
      println("XXXXXXXXXX")
      return
    }

    if(old.equals(this.root))
    {
       this.root=branch
       return
    }

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
   * find sub branch to insert
   * @param b
   * @param p
   * @return
   */
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

  /**
   * Insert entries into the tree, returning a new tree.
   */
  def insertAll(entries: Iterable[Entry[V]])=
  {
    entries.foreach(e=>this.insert(e))
  }

  /**
   * Insert entries into the tree, returning a new tree.
   */
  def insertAll(entries: Iterable[Entry[V]], f:(V,V)=>V)=
  {
    entries.foreach(e=>this.insert(e,f))
  }

  /**
   * Remove an entry from the tree, returning a new tree.
   *
   * If the entry was not present, the tree is simply returned.
   */
  def remove(entry: Entry[V]):Boolean=
  {
      this.remove(entry,this.root)
  }

  private def remove(entry: Entry[V], node:Node):Boolean={

    node match {
      case l: leafwithstorage[V] =>
        if(l.removeEntry(entry))
        {
          size-=1
          true
        }else
        {
          false
        }

      case b: Branch => {
        val subbranch=this.findSubchildren(b,entry.geom)
        remove(entry,subbranch)
      }
    }

  }


  /**
   * Remove entries from the tree, returning a new tree.
   */
  def removeAll(entries: Iterable[Entry[V]]) =
  {
    entries.foreach(e=>this.remove(e))
  }

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def search(space: Box): Seq[Entry[V]] =
  {
    //val ret=ArrayBuffer.empty[Entry[V]]
    this.search(space,(id)=>true)
  }

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def search(space: Box, f: Entry[V] => Boolean): Seq[Entry[V]] =
  {
    val ret=ArrayBuffer.empty[Entry[V]]

    def recur(space: Box, node:Node, f: Entry[V] => Boolean):Unit=
    {
      node match
      {
        case l:leafwithstorage[V]=>
          if(l.getbox.intersects(space))
          {
            l.storage.foreach{
              case(e:Geom,v)=>
                if(space.contains(e)&&f(Entry[V](e,v)))
                {
                  ret.append(Entry[V](e,v))
                }
            }
          }
        case b:Branch=>
        {
          b.findChildNodes(space).foreach(child=>recur(space,child,f))
        }
      }
    }
    recur(space,this.root,f)
    ret
  }




  /**
   * Return a sequence of all entries found for an given point.
   */
  def searchPoint(pt: Point): Entry[V] =
  {
        this.searchPoint(pt,this.root).get
  }

  private def searchPoint(pt: Point, node:Node): Option[Entry[V]] =
  {
       node match
       {
         case l:leafwithstorage[V]=>
               l.getEntry(pt)
         case b:Branch=>
         {
           val subbranch=this.findSubchildren(b,pt)
           searchPoint(pt,subbranch)
         }
       }
  }


  private def findPointLeaf(pt: Point, node:Node): Option[Node] =
  {
    node match
    {
      case l:leafwithstorage[V]=>
        if(l.getbox.contains(pt))
        {
          Some(l)
        }else{
          None
        }
      case b:Branch=>
      {
        val subbranch=this.findSubchildren(b,pt)
        findPointLeaf(pt,subbranch)
      }
    }
  }

  /**
   * Return the nearest point for an given point.
   */
  def nearest(pt: Point): Option[Entry[V]] =
  {
    findPointLeaf(pt,this.root).getOrElse(None)
      match
    {
      case l:leafwithstorage[V]=>{
        var dist=Double.PositiveInfinity

        var ret=Point(0,0)
        l.storage.foreach { e =>
          val d = e._1.distance(pt)
          if (d < dist) {
            dist=d
            ret=e._1.asInstanceOf[Point]
          }
        }

      l.getEntry(ret)

      }
      case _=>None
    }
  }

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearestK(pt: Point, k: Int): Iterator[Entry[V]] =
  {
    nearestK(pt,k,(id)=>true)
  }

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearestK(pt: Point,k: Int, z:Entry[V]=>Boolean): Iterator[Entry[V]] = {

    if (k < 1) {
      Vector.empty.toIterator
    } else {

      implicit val ord = Ordering.by[(Double, Entry[V]), Double](_._1)
      val pq = PriorityQueue.empty[(Double, Entry[V])]
      nearestK(this.root,pt, k, Double.PositiveInfinity, pq)
      val arr = new Array[Entry[V]](pq.size)
      var i = arr.length - 1
      while (i >= 0) {
        val (_, e) = pq.dequeue
        arr(i) = e
        i -= 1
      }
      arr.toIterator
    }
  }

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearestKwithDistance(pt: Point,k: Int,z:Entry[V]=>Boolean): IndexedSeq[(Double,Entry[V])] = {
    if (k < 1) {
      Vector.empty
    } else {
      implicit val ord = Ordering.by[(Double, Entry[V]), Double](_._1)
      val pq = PriorityQueue.empty[(Double, Entry[V])]
      nearestK(this.root,pt, k, Double.PositiveInfinity, pq)
      pq.toIndexedSeq
    }
  }

  private def nearestK(node:Node, pt: Point, k: Int, d0: Double, pq: PriorityQueue[(Double, Entry[V])]):
  Double = {
    var dist: Double = d0

    node match {
      case l:leafwithstorage[V] =>
        l.storage.foreach { e =>
          val d = e._1.distance(pt)
          if (d < dist) {
            pq += ((d, Entry(e._1,e._2)))
            if (pq.size > k)
            {
              pq.dequeue
              dist = pq.head._1
            }
          }
        }
      case b:Branch =>
        val cs = b.children.map(node => (node.getbox.distance(pt), node)).toList.sortBy(_._1)
        cs.foreach {
          case (d, node) =>
          if (d >= dist) return dist //scalastyle:ignore
          dist = nearestK(node, pt, k, dist, pq)
        }
    }
    dist
  }

  def contains(pt:Geom):Boolean={

    contains(pt,(id)=>true)

  }

  def contains(pt:Geom, f:Entry[V]=>Boolean):Boolean={

     def recur(node:Node, pt:Geom, f:Entry[V]=>Boolean): Boolean =
    {
      node match {
        case l: leafwithstorage[V] =>
          l.storage.contains(pt) && f(l.getEntry(pt).get)
        case b: Branch =>
          recur(this.findSubchildren(b, pt), pt, f)
      }
    }
    recur(this.root,pt,f)
  }

  /**
   * Return whether or not the value exists in the tree at (x, y).
   */
  def containEntry(x: Float, y: Float, value: V): Boolean =
  {
    containEntry(this.root,Entry(Point(x,y),value),(id)=>true)
  }

  /**
   * Return whether or not the given entry exists in the tree.
   */
  def containEntry(entry: Entry[V]): Boolean =
  {
    containEntry(this.root,entry,(id)=>true)
  }

  /**
   * Return whether or not the given entry exists in the tree.
   */
  def containEntry[K](k:K, value:V): Boolean =
  {
    containEntry(this.root,Entry(k.asInstanceOf[Geom],value),(id)=>true)
  }

  def containEntry(entry: Entry[V], f:Entry[V]=>Boolean): Boolean =
  {
      containEntry(this.root,entry,f)
  }

  private def containEntry(node:Node, entry: Entry[V], f:Entry[V]=>Boolean): Boolean =
  {
    node match
    {
      case l:leafwithstorage[V]=>

         if(l.storage.contains(entry.geom))
         {
           val e=l.getEntry(entry.geom).get
           f(e)
         }else
         {
           false
         }
      case b:Branch=>
      {
        val subbranch=this.findSubchildren(b,entry.geom)
        containEntry(subbranch,entry,f)
      }
    }

  }


  /**
   * Return an iterator over all entries in the tree.
   */
  def entries: Iterator[Entry[V]] =
  {

    val itr=ArrayBuffer.empty[Entry[V]]

    def iterator(node:Node): Unit =
      node match {
        case l:leafwithstorage[V] =>
          itr.appendAll(l.storage.map{
            case(e:Geom,v)=>Entry[V](e,v)
          })

        case b:Branch =>
          b.children.foreach{
            n=>iterator(n)
          }
      }

    iterator(this.root)
    itr.toIterator
  }

  /**
   * Returns an iterator over all the entires this node contains
   * (directly or indirectly). Since nodes are immutable there is no
   * concern over concurrent updates while using the iterator.
   */



  def printStructure(): Unit =
  {
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
        case l: leafwithstorage[V] =>
          print(" L(" + l.storage.size + ") ")
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

  override def toString: String =
    s"RTree(<$size entries>)"
}

object QTree
{
  /**
   * Construct an empty RTree.
   */
  def empty[V]: QTree[V] = QTree(qtreeUtil.MaxLeafBound, qtreeUtil.wholespace)
  /**
   * Construct an RTree from a sequence of entries.
   */
  def apply[V](entries: Entry[V]*): QTree[V] =
  {
    val qtree=QTree.empty[V]
    qtree.insertAll(entries)
    qtree
  }

  def apply[V](entries: Iterator[Entry[V]]): QTree[V] =
  {
    val qtree=QTree.empty[V]
    qtree.insertAll(entries.toIterable)
    qtree
  }

}
