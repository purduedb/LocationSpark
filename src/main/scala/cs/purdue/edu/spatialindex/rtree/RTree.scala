package cs.purdue.edu.spatialindex.rtree

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet, PriorityQueue}
import scala.reflect.ClassTag
import scala.util.Try

object RTree {

  /**
   * Construct an empty RTree.
   */
  def empty[V]: RTree[V] = new RTree(Node.empty[V], 0)

  /**
   * Construct an RTree from a sequence of entries.
   */
  def apply[V](entries: Entry[V]*): RTree[V] =
    entries.foldLeft(RTree.empty[V])(_ insert _)

  def apply[A](itr:Iterator[Entry[A]])=
  {
    itr.foldLeft(RTree.empty[A])(_ insert _)
  }

  def apply[A](itr:Iterable[Entry[A]])=
  {
    itr.foldLeft(RTree.empty[A])(_ insert _)
  }
}

/**
 * This is the magnificent RTree, which makes searching ad-hoc
 * geographic data fast and fun.
 *
 * The RTree wraps a node called 'root' that is the actual root of the
 * tree structure. RTree also keeps track of the total size of the
 * tree (something that individual nodes don't do).
 */
case class RTree[V](root: Node[V], size: Int) {

  private var tree_sorted=false
  /**
   * Typesafe equality test.
   *
   * In order to be considered equal, two trees must have the same
   * number of entries, and each entry found in one should be found in
   * the other.
   */
  def ===(that: RTree[V]): Boolean =
    size == that.size && entries.forall(that.contains)

  /**
   * Universal equality test.
   *
   * Trees can only be equal to other trees. Unlike some other
   * containers, the trees must be parameterized on the same type, or
   * else the comparison will fail.
   *
   * This means comparing an RTree[Int] and an RTree[BigInt] will
   * always return false.
   */
  override def equals(that: Any): Boolean =
    that match {
      case rt: RTree[_] =>
        Try(this === rt.asInstanceOf[RTree[V]]).getOrElse(false)
      case _ =>
        false
    }

  /**
   * Universal hash code method.
   */
  override def hashCode(): Int = {
    var x = 0xbadd0995
    val it = entries
    while (it.hasNext) x ^= (it.next.hashCode * 777 + 1)
    x
  }

  /**
   * Insert a value into the tree at (x, y), returning a new tree.
   */
  def insert(x: Float, y: Float, value: V): RTree[V] =
    insert(Entry(Point(x, y), value))


  /**
   * Insert an entry into the tree, returning a new tree.
   */
  def insert(entry: Entry[V]): RTree[V] = {
    val r = root.insert(entry) match {
      case Left(rs) => Branch(rs, rs.foldLeft(Box.empty)(_ expand _.box))
      case Right(r) => r
    }
    RTree(r, size + 1)
  }

  /**
   * Insert a value into the tree at (x, y), returning a new tree.
   */
  def insert(x:Point, value: V): RTree[V] =
    insert(Entry(x, value))


  /**
   * Insert entries into the tree, returning a new tree.
   */
  def insertAll(entries: Iterable[Entry[V]]): RTree[V] =
    entries.foldLeft(this)(_ insert _)

  /**
   * Remove an entry from the tree, returning a new tree.
   *
   * If the entry was not present, the tree is simply returned.
   */
  def remove(entry: Entry[V]): RTree[V] =
    root.remove(entry) match {
      case None =>
        this
      case Some((es, None)) =>
        es.foldLeft(RTree.empty[V])(_ insert _)
      case Some((es, Some(node))) =>
        es.foldLeft(RTree(node, size - es.size - 1))(_ insert _)
    }

  /**
   * Remove entries from the tree, returning a new tree.
   */
  def removeAll(entries: Iterable[Entry[V]]): RTree[V] =
    entries.foldLeft(this)(_ remove _)

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def search(space: Box): Seq[Entry[V]] =
    root.search(space, _ => true)

  /**
   * this is the dual tree join algorithm,
   * this one the data tree, and stree is the query tree.
   * @param stree
   * @return
   */
  def join(stree:RTree[V]):Seq[Entry[V]] =
  {
    val buf = HashSet.empty[Entry[V]]

    //this changed to generic search
    //recursive
    //case 3: the data tree is leaf, but the query tree is branch
    def recur(node: Node[V], entry:Entry[V]): Unit =
      node match {
      case Leaf(children, box) =>
        children.foreach { c =>
          if (c.geom.contains(entry.geom))
             buf.add(entry)
        }
      case Branch(children, box) =>
        children.foreach { c =>
          if (c.box.intersects(entry.geom)) recur(c,entry)
        }
    }

    //this changed to generic search
    //recursive to the leaf node, which contain the data itself
    //case 4: the data tree is branch, but the query tree is leaf
    def recur2(node: Node[V], querybox:Geom): Unit =
      node match {
        case Leaf(children, box) =>
          children.foreach { c =>
            if (querybox.contains(c.geom))
              buf.add(c)
          }
        case Branch(children, box) =>
          children.foreach { c =>
            if (querybox.intersects(c.geom)) recur2(c,querybox)
          }
      }

    //this is used for spatial join
    def sjoin(rnode:Node[V],snode:Node[V]):Unit={

      if(rnode.box.intersects(snode.box))
      {
         val intesectionbox=rnode.box.intesectBox(snode.box)

        rnode match {

          case Leaf(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 2: both tree reach the leaf node

                children_r.foreach{
                  cr=>
                    children_s.foreach {
                      cs =>
                        if (cs.geom.contains(cr.geom))
                          buf.add(cr) //return the results
                    }
                }

                /*intersectionForleaf(children_s,children_s).foreach {
                  case (cr, cs) =>
                    if(cs.geom.contains(cr.geom))
                    buf.append(cr) //return the results
                }*/


              case Branch(children_s, box_s)=> //case 3: the data tree is leaf, but the query tree is branch
              {
                val r=children_r.filter(entry=>entry.geom.intersects(intesectionbox))
                val s=children_s.filter(node=>node.box.intersects(intesectionbox))

                r.foreach{
                  cr=>
                    s.foreach {
                      cs =>
                        recur(cs,cr)
                    }
                }

              }
            }

          case Branch(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 4: the data tree is branch, but the query tree is leaf

                val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                val s=children_s.filter(entry=>entry.geom.intersects(intesectionbox))

                r.foreach{
                  cr=>
                    s.foreach {
                      cs =>
                        recur2(cr,cs.geom)
                    }
                }

              case Branch(children_s, box_s)=> //case 1: both of two tree are branch
              {
                  val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                  val s=children_s.filter(node=>node.box.intersects(intesectionbox))
                //nest loop to recursive on those dataset

                //use the sorted based approach for the intersection aprt
                //by this way, the nest-loop is changed to o(n)+k

                /*intersection(r,s).foreach
                {
                  case(rc,sc)=>
                    sjoin(rc,sc)
                }*/

                  r.foreach {
                    rc =>
                    s.foreach{
                      sc=>
                        if(rc.box.intersects(sc.box))
                          sjoin(rc,sc)
                    }
                  }

                //next step to test on sort and plan sweep approach
              }
            }
        }
      }

    }

    sjoin(this.root,stree.root)

    buf.toSeq
  }


  /**
   * this is the dual tree join algorithm,
   * this one the data tree, and stree is the query tree.
   * the return is the
   * query box w.r.t the point1, point2, point3...
   * @param stree
   * @return
   */
  def joins_withoutsort[K: ClassTag, U:ClassTag, U2: ClassTag](stree:RTree[V])(
    f: (Iterator[(K,V)]) => U2,f2:(U2,U2)=>U2):
  mutable.HashMap[U,U2] =
  {
    val buf = mutable.HashMap.empty[Geom,ArrayBuffer[(K,V)]]

    val tmpresult = mutable.HashMap.empty[Geom,U2]

    def updatehashmap(key:Geom, value:Entry[V])=
    {
      try {
        if(buf.contains(key))
        {
          val tmp1=buf.get(key).get

          //this is used in case the tmp buffer is too big
          if(tmp1.size>10)
          {
             val result=f(tmp1.toIterator)
             tmp1.clear()
             if(tmpresult.contains(key))
             {
               val result2=tmpresult.get(key).get
               //aggregate the internal result
               tmpresult.put(key,f2(result2,result))
             }else
             {
               tmpresult.put(key,result)
             }
          }

          tmp1.append((value.geom.asInstanceOf[K]->value.value))
          buf.put(key,tmp1)
        }else
        {
          val tmp1=new ArrayBuffer[(K,V)]
          tmp1.append((value.geom.asInstanceOf[K]->value.value))
          buf.put(key,tmp1)
        }


      }catch
        {
          case e:Exception=>
          println("out of memory for appending new value to the sjoin")
        }
    }
    //this changed to generic search
    //recursive
    //case 3: the data tree is leaf, but the query tree is branch
    def recur(node: Node[V], entry:Entry[V]): Unit =
      node match {
        case Leaf(children, box) =>
          children.foreach { c =>
            if (c.geom.contains(entry.geom))
            {
              updatehashmap(c.geom,entry)
            }

          }
        case Branch(children, box) =>
          children.foreach { c =>
            if (c.box.intersects(entry.geom)) recur(c,entry)
          }
      }

    //this changed to generic search
    //recursive to the leaf node, which contain the data itself
    //case 4: the data tree is branch, but the query tree is leaf
    def recur2(node: Node[V], querybox:Geom): Unit =
      node match {
        case Leaf(children, box) =>
          children.foreach { c =>
            if (querybox.contains(c.geom))
              updatehashmap(querybox,c)
          }
        case Branch(children, box) =>
          children.foreach { c =>
            if (querybox.intersects(c.geom)) recur2(c,querybox)
          }
      }
    //this is used for spatial join
    def sjoin(rnode:Node[V],snode:Node[V]):Unit={

      if(rnode.box.intersects(snode.box))
      {
        val intesectionbox=rnode.box.intesectBox(snode.box)

        rnode match {

          case Leaf(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 2: both tree reach the leaf node

                children_r.foreach{
                  cr=>
                    children_s.foreach {
                      cs =>
                        if (cs.geom.contains(cr.geom))
                        {
                          updatehashmap(cs.geom,cr)
                        }//if

                    }
                }

              case Branch(children_s, box_s)=> //case 3: the data tree is leaf, but the query tree is branch
              {
                val r=children_r.filter(entry=>entry.geom.intersects(intesectionbox))
                val s=children_s.filter(node=>node.box.intersects(intesectionbox))

                r.foreach{
                  cr=>
                    s.foreach {
                      cs =>
                        recur(cs,cr)
                    }
                }

              }
            }

          case Branch(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 4: the data tree is branch, but the query tree is leaf

                val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                val s=children_s.filter(entry=>entry.geom.intersects(intesectionbox))

                r.foreach{
                  cr=>
                    s.foreach {
                      cs =>
                        recur2(cr,cs.geom)
                    }
                }

              case Branch(children_s, box_s)=> //case 1: both of two tree are branch
              {
                val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                val s=children_s.filter(node=>node.box.intersects(intesectionbox))

                r.foreach {
                  rc =>
                    s.foreach{
                      sc=>
                        if(rc.box.intersects(sc.box))
                          sjoin(rc,sc)
                    }
                }

                //next step to test on sort and plan sweep approach
              }
            }
        }
      }

    }

    sjoin(this.root,stree.root)

    buf.map{
      case(geom,itr)=>
        val t1=f(itr.toIterator)
        val t2=tmpresult.get(geom).getOrElse(None)
        t2 match
        {
          case t:U2=> (geom.asInstanceOf[U],f2(t1,t))
          case _=>(geom.asInstanceOf[U],t1)
        }
    }

  }

  /**
   * sort the internal node of this RTree
   * if this tree is not sorted, return a new tree
   * if this tree is sorted, return itself
   * @return
   */
  def sortInternalnode(): RTree[V] =
  {
     def recursort(node:Node[V]):Node[V]=
     {
       node match {
         case Leaf(children, box) =>
             node
         case Branch(children, box) =>
         {
           val tmpchild=new ArrayBuffer[Node[V]]()
           children.foreach
            {
             e=>
               tmpchild.append(recursort(e))
            }
           Branch(tmpchild.sortBy(e=>e.geom.x).toVector,box)
         }
       }
     }

    if(tree_sorted==false) {
       val newroot=recursort(this.root)
      val newtree=new RTree(newroot,this.size)
      newtree.tree_sorted=true
      newtree
    }else
    {
      this
    }

  }

  /**
   * this is the dual tree join for knn join algorithm,
   * this one the data tree, and stree is the query tree
   * @param stree
   * @return
   */
  /*def rjoinforknn[K: ClassTag]
  ( stree:RTree[V],
    boxpointmap:mutable.HashMap[Geom,(K,Double,mutable.PriorityQueue[(Double,(K,V))])]
    ,f1:(K)=>Boolean,
    f2:(V)=>Boolean
    ):
  Iterator[(K,Array[(K,V)])]=
  {
    stree.sortInternalnode()
    this.sortInternalnode()

    val buf=boxpointmap

    def updatehashmap(key:Geom, value:Entry[V])=
    {
      try {

        if(buf.contains(key))
        {
          val (querypoint, maxdistance, pq)=buf.get(key).get

          val distance=querypoint.asInstanceOf[Geom].distance(value.geom.asInstanceOf[Point])

          if(distance<maxdistance)
          {
            pq += ((distance,(value.geom.asInstanceOf[K],value.value)))
            pq.dequeue
            buf.put(key,(querypoint,distance,pq))
          }
        }

      }catch
        {
          case e:Exception=>
            println(e)
            println("out of memory for appending new value to the sjoin")
        }
    }

    def recur(node: Node[V], entry:Entry[V]): Unit =
      node match {
        case Leaf(children, box) =>
          children.foreach { c =>
            if (c.geom.contains(entry.geom))
            {
              updatehashmap(c.geom,entry)
            }

          }
        case Branch(children, box) =>
          children.foreach { c =>
            if (c.box.intersects(entry.geom)) recur(c,entry)
          }
      }

    def recur2(node: Node[V], querybox:Geom): Unit =
      node match {
        case Leaf(children, box) =>
          children.foreach { c =>
            if (querybox.contains(c.geom))
              updatehashmap(querybox,c)
          }
        case Branch(children, box) =>
          children.foreach { c =>
            if (querybox.intersects(c.geom)) recur2(c,querybox)
          }
      }


    //this is used for spatial join
    def sjoin(rnode:Node[V],snode:Node[V]):Unit={

      if(rnode.box.intersects(snode.box))
      {
        val intesectionbox=rnode.box.intesectBox(snode.box)

        rnode match {

          case Leaf(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 2: both tree reach the leaf node

                val r=children_r.filter(entry=>entry.geom.intersects(intesectionbox))
                val s=children_s.filter(entry=>entry.geom.intersects(intesectionbox))

                intersectionForleaf(r,s).foreach {
                  case (cr, cs) =>
                    if(cs.geom.contains(cr.geom))
                      updatehashmap(cs.geom,cr)
                }


              case Branch(children_s, box_s)=> //case 3: the data tree is leaf, but the query tree is branch
              {
                val r=children_r.filter(entry=>entry.geom.intersects(intesectionbox))
                val s=children_s.filter(node=>node.box.intersects(intesectionbox))

                r.foreach{
                  cr=>
                    s.foreach {
                      cs =>
                        recur(cs,cr)
                    }
                }

              }
            }

          case Branch(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 4: the data tree is branch, but the query tree is leaf

                val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                val s=children_s.filter(entry=>entry.geom.intersects(intesectionbox))

                r.foreach{
                  cr=> s.foreach {
                    cs => recur2(cr,cs.geom)
                  }
                }

              case Branch(children_s, box_s)=> //case 1: both of two tree are branch
              {
                val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                val s=children_s.filter(node=>node.box.intersects(intesectionbox))

                intersection(r,s).foreach
                {
                  case(rc,sc)=>
                    sjoin(rc,sc)
                }
              }
            }
        }
      }

    }

    sjoin(this.root,stree.root)


   buf.map
    {
      case(geom,(querypoint,distance, pq))=>
        val arr = ArrayBuffer.empty[(K,V)]
        while (!pq.isEmpty) {
          val (d, (key,value)) = pq.dequeue
          if(f1(key)&&f2(value))
            try
            {
              arr.append((key,value))
            }catch
              {
                case e:Exception=>
                  println("out of memory for returing results exception"+e)
              }
        }
        pq.clear()
        (querypoint, arr.toArray)
    }.toIterator

  }*/

  /**
   * this is the dual tree join algorithm based on sorting and intersection of rectangles,
   * this one the data tree, and stree is the query tree.
   * the return is the
   * query box w.r.t the point1, point2, point3...
   * @param stree
   * @return
   */
  def joins[K: ClassTag, U:ClassTag, U2: ClassTag](stree:RTree[V])(
    f: (Iterator[(K,V)]) => U2,f2:(U2,U2)=>U2):
  mutable.HashMap[U,U2] =
  {



    val buf = mutable.HashMap.empty[Geom,ArrayBuffer[(K,V)]]

    val tmpresult = mutable.HashMap.empty[Geom,U2]

    def updatehashmap(key:Geom, value:Entry[V])=
    {
      try {
        if(buf.contains(key))
        {
          val tmp1=buf.get(key).get

          //this is used in case the tmp buffer is too big
          if(tmp1.size>20)
          {
            val result=f(tmp1.toIterator)
            tmp1.clear()
            if(tmpresult.contains(key))
            {
              val result2=tmpresult.get(key).get
              //aggregate the internal result
              tmpresult.put(key,f2(result2,result))
            }else
            {
              tmpresult.put(key,result)
            }
          }

          tmp1.append((value.geom.asInstanceOf[K]->value.value))
          buf.put(key,tmp1)
        }else
        {
          val tmp1=new ArrayBuffer[(K,V)]
          tmp1.append((value.geom.asInstanceOf[K]->value.value))
          buf.put(key,tmp1)
        }


      }catch
        {
          case e:Exception=>
            println("out of memory for appending new value to the sjoin")
        }
    }
    //this changed to generic search
    //recursive
    //case 3: the data tree is leaf, but the query tree is branch
    def recur(node: Node[V], entry:Entry[V]): Unit =
      node match {
        case Leaf(children, box) =>
          children.foreach { c =>
            if (c.geom.contains(entry.geom))
            {
              updatehashmap(c.geom,entry)
            }

          }
        case Branch(children, box) =>
          children.foreach { c =>
            if (c.box.intersects(entry.geom)) recur(c,entry)
          }
      }

    //this changed to generic search
    //recursive to the leaf node, which contain the data itself
    //case 4: the data tree is branch, but the query tree is leaf
    def recur2(node: Node[V], querybox:Geom): Unit =
      node match {
        case Leaf(children, box) =>
          children.foreach { c =>
            if (querybox.contains(c.geom))
              updatehashmap(querybox,c)
          }
        case Branch(children, box) =>
          children.foreach { c =>
            if (querybox.intersects(c.geom)) recur2(c,querybox)
          }
      }


    //this is used for spatial join
    def sjoin(rnode:Node[V],snode:Node[V]):Unit={

      if(rnode.box.intersects(snode.box))
      {
        val intesectionbox=rnode.box.intesectBox(snode.box)

        rnode match {

          case Leaf(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 2: both tree reach the leaf node

                val r=children_r.filter(entry=>entry.geom.intersects(intesectionbox))
                val s=children_s.filter(entry=>entry.geom.intersects(intesectionbox))

                intersectionForleaf(r,s).foreach {
                  case (cr, cs) =>
                    if(cs.geom.contains(cr.geom))
                      updatehashmap(cs.geom,cr)
                }

              case Branch(children_s, box_s)=> //case 3: the data tree is leaf, but the query tree is branch
              {
                val r=children_r.filter(entry=>entry.geom.intersects(intesectionbox))
                val s=children_s.filter(node=>node.box.intersects(intesectionbox))

                //var b1=System.currentTimeMillis

                r.foreach{
                  cr=>
                    s.foreach {
                      cs =>
                        recur(cs,cr)
                    }
                }
                //println("the case 2 loop time: "+(System.currentTimeMillis-b1) +" ms")
                //println("!!!!!!!!!!"*100)
              }
            }

          case Branch(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 4: the data tree is branch, but the query tree is leaf

                val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                val s=children_s.filter(entry=>entry.geom.intersects(intesectionbox))

                r.foreach{
                  cr=> s.foreach {
                      cs => recur2(cr,cs.geom)
                    }
                }

              case Branch(children_s, box_s)=> //case 1: both of two tree are branch
              {
                val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                val s=children_s.filter(node=>node.box.intersects(intesectionbox))

                intersection(r,s).foreach
                {
                  case(rc,sc)=>
                    sjoin(rc,sc)
                }

              }
            }
        }
      }

    }

    val queries=stree.sortInternalnode()
    val dataset=this.sortInternalnode()

    sjoin(dataset.root,queries.root)

    buf.map{
      case(geom,itr)=>
        val t1=f(itr.toIterator)
        val t2=tmpresult.get(geom).getOrElse(None)
        t2 match
        {
          case None=>(geom.asInstanceOf[U],t1)
          case _ => (geom.asInstanceOf[U],f2(t1,t2.asInstanceOf[U2]))
        }
    }

  }

  /**
   * this is the dual tree join algorithm based on sorting and intersection of rectangles,
   * this one the data tree, and stree is the query tree.
   * this approach is used for knn join
   * the return is the
   * query point w.r.t the point1, point2, point3...
   * @param stree
   * @return
   */
  def sjoinfor_knnjoin[K: ClassTag](stree:RTree[V],boxMapData:Map[Box,Iterable[Point]], knn:Int)(
    f: (K,V) => Boolean):
  mutable.HashMap[Point,Iterable[(K,V)]] =
  {

    stree.sortInternalnode()
    this.sortInternalnode()

    val buf = mutable.HashMap.empty[Point,mutable.PriorityQueue[(Double,(K,V))]]

    def updatehashmap(querybox:Box, value:Entry[V])=
    {
      try {
           boxMapData.get(querybox).get.foreach
           {
             case knnquerypoint=>
               if(buf.contains(knnquerypoint))
               {
                 val pq=buf.get(knnquerypoint).get
                 val d = value.geom.distance(knnquerypoint.asInstanceOf[Point])
                 //update the priority queue for this query point
                 if(pq.size>=knn)
                 {
                   var maxdist = pq.head._1
                   if (d < maxdist) {
                     pq += ((d, (value.geom.asInstanceOf[K],value.value)))
                     if (pq.size > knn) {
                       pq.dequeue
                       maxdist = pq.head._1
                     }
                   }
                 }else
                 {
                   pq += ((d, (value.geom.asInstanceOf[K],value.value)))
                 }

                 buf.put(knnquerypoint,pq)

               }else
               {
                 implicit val ord = Ordering.by[(Double, (K,V)), Double](_._1)
                 val pq = PriorityQueue.empty[(Double, (K,V))]
                 val d = value.geom.distance(knnquerypoint.asInstanceOf[Point])
                 pq += ((d, (value.geom.asInstanceOf[K],value.value)))
                 buf.put(knnquerypoint,pq)
               }
        }
      }catch
        {
          case e:Exception=>
            println("the data type is not correct and out of memory for intermediate results")
        }
    }

    //this changed to generic search
    //recursive
    //case 3: the data tree is leaf, but the query tree is branch
    def recur(node: Node[V], entry:Entry[V]): Unit =
      node match {
        case Leaf(children, box) =>
          children.foreach { c =>
            if (c.geom.contains(entry.geom)&&f(entry.geom.asInstanceOf[K],entry.value))
            {
              updatehashmap(c.geom.asInstanceOf[Box],entry)
            }

          }
        case Branch(children, box) =>
          children.foreach { c =>
            if (c.box.intersects(entry.geom)) recur(c,entry)
          }
      }

    //this changed to generic search
    //recursive to the leaf node, which contain the data itself
    //case 4: the data tree is branch, but the query tree is leaf
    def recur2(node: Node[V], querybox:Geom): Unit =
      node match {
        case Leaf(children, box) =>
          children.foreach { c =>
            if (querybox.contains(c.geom)&&f(c.geom.asInstanceOf[K],c.value))
              updatehashmap(querybox.asInstanceOf[Box],c)
          }
        case Branch(children, box) =>
          children.foreach { c =>
            if (querybox.intersects(c.geom)) recur2(c,querybox)
          }
      }


    //this is used for spatial join
    def sjoin(rnode:Node[V],snode:Node[V]):Unit={

      if(rnode.box.intersects(snode.box))
      {
        val intesectionbox=rnode.box.intesectBox(snode.box)

        rnode match {

          case Leaf(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 2: both tree reach the leaf node

                val r=children_r.filter(entry=>entry.geom.intersects(intesectionbox))
                val s=children_s.filter(entry=>entry.geom.intersects(intesectionbox))

                intersectionForleaf(r,s).foreach {
                  case (cr, cs) =>
                    if(cs.geom.contains(cr.geom))
                      updatehashmap(cs.geom.asInstanceOf[Box],cr)
                }

              case Branch(children_s, box_s)=> //case 3: the data tree is leaf, but the query tree is branch
              {
                val r=children_r.filter(entry=>entry.geom.intersects(intesectionbox))
                val s=children_s.filter(node=>node.box.intersects(intesectionbox))

                //var b1=System.currentTimeMillis

                r.foreach{
                  cr=>
                    s.foreach {
                      cs =>
                        recur(cs,cr)
                    }
                }
                //println("the case 2 loop time: "+(System.currentTimeMillis-b1) +" ms")
                //println("!!!!!!!!!!"*100)
              }
            }

          case Branch(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 4: the data tree is branch, but the query tree is leaf

                val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                val s=children_s.filter(entry=>entry.geom.intersects(intesectionbox))

                r.foreach{
                  cr=> s.foreach {
                    cs => recur2(cr,cs.geom)
                  }
                }

              case Branch(children_s, box_s)=> //case 1: both of two tree are branch
              {
                val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                val s=children_s.filter(node=>node.box.intersects(intesectionbox))

                intersection(r,s).foreach
                {
                  case(rc,sc)=>
                    sjoin(rc,sc)
                }

              }
            }
        }
      }

    }

    sjoin(this.root,stree.root)

    buf.map
    {
      case(querypoint,pq)=>
        (querypoint,pq.map{case(distance,(k,v))=>(k,v)}.toIterable)
    }

  }



  //use the line swipe based approach to find the intersection of two rectangle sets
  private def intersection(r:Vector[Node[V]],s:Vector[Node[V]]):Seq[(Node[V],Node[V])]=
  {
    //val rsort=r.sortBy(e=>e.geom.x)
    //val ssort=s.sortBy(e=>e.geom.x)

    val buf = ArrayBuffer.empty[(Node[V], Node[V])]

    def internalloop(entry:Node[V], marked:Int, label:Boolean, s:Vector[Node[V]]): Unit =
    {
      var i=marked
      while(i<s.size&&s(i).geom.x<=entry.geom.x2)
      {
        if(entry.geom.y<=s(i).geom.y2&&entry.geom.y2>=s(i).geom.y)
        {
          if(label==true)
            buf.+=(entry->s(i))
          else
            buf.+=(s(i)->entry)
        }
        i+=1
      }
    }

    var i=0
    var j=0

    while(i<r.size&&j<s.size)
    {
      if(r(i).geom.x<s(j).geom.x)
      {
        internalloop(r(i),j,true,s)
        i+=1
      }else
      {
        internalloop(s(j),i,false,r)
        j+=1
      }
    }
    buf
  }

  //use the line swipe based approach to find the intersection of two rectangle sets
 /*
  private def intersection_knn(r:Vector[Node[V]],s:Vector[Node[V]]):(Seq[(Double, Node[V],Node[V])],Seq[(Double, Node[V],Node[V])])=
  {
    //val rsort=r.sortBy(e=>e.geom.x)
    //val ssort=s.sortBy(e=>e.geom.x)

    val buf_intersection = ArrayBuffer.empty[(Double, Node[V], Node[V])]

    val buf_nonintersection = ArrayBuffer.empty[(Double, Node[V], Node[V])]

    r.foreach
    {
      case rnode=>

        s.foreach
        {
          case snode=>

            if(rnode.box.intersects(snode.box))
            {
              buf_intersection.+=((rnode.box.intersectionarea(snode.box), rnode, snode))
            }
            else
            {
              buf_nonintersection.+=((rnode.box.mindistance(snode.box), rnode, snode))
            }
        }
    }

    (buf_intersection.toSeq.sortBy(_._1)(Ordering[Double].reverse),buf_nonintersection.toSeq)
  }


  //use the line swipe based approach to find the intersection of two rectangle sets
  private def intersection_knn_sorted(r:Vector[Node[V]],s:Vector[Node[V]]):(Seq[(Double, Node[V],Node[V])],Seq[(Double, Node[V],Node[V])])=
  {
    //val rsort=r.sortBy(e=>e.geom.x)
    //val ssort=s.sortBy(e=>e.geom.x)

    val buf_intersection = ArrayBuffer.empty[(Double, Node[V], Node[V])]

    val buf_nonintersection = ArrayBuffer.empty[(Double, Node[V], Node[V])]

    r.foreach
    {
      case rnode=>

        s.foreach
        {
          case snode=>

            if(rnode.box.intersects(snode.box))
            {
              buf_intersection.+=((rnode.box.intersectionarea(snode.box), rnode, snode))
            }
            else
            {
              buf_nonintersection.+=((rnode.box.mindistance(snode.box), rnode, snode))
            }
        }
    }

    (buf_intersection.toSeq.sortBy(_._1)(Ordering[Double].reverse),buf_nonintersection.toSeq)
  }*/

  //use the line swipe based approach to find the intersection of two rectangle sets
  private def intersectionForleaf(r:Vector[Entry[V]],s:Vector[Entry[V]]):Seq[(Entry[V],Entry[V])]=
  {
    //val rsort=r.sortBy(e=>e.geom.x)
    //val ssort=s.sortBy(e=>e.geom.x)

    val buf = ArrayBuffer.empty[(Entry[V], Entry[V])]

    def internalloop(entry:Entry[V], marked:Int, label:Boolean, s:Vector[Entry[V]]): Unit =
    {
      var i=marked
      while(i<s.size&&s(i).geom.x<=entry.geom.x2)
      {
        if(entry.geom.y<=s(i).geom.y2&&entry.geom.y2>=s(i).geom.y)
        {
          if(label)
          buf.+=(entry->s(i))
          else
            buf.+=(s(i)->entry)
        }
        i+=1
      }
    }

    var i=0
    var j=0

    while(i<r.size&&j<s.size)
    {
      if(r(i).geom.x<s(j).geom.x)
      {
        internalloop(r(i),j,true,s)
        i+=1
      }else
      {
        internalloop(s(j),i,false,r)
        j+=1
      }
    }
    buf
  }

  def cleanTree()=
  {
    def recur(node:Node[V]):Unit= {

      node match {
        case Leaf(children, box) =>
          children.drop(children.size - 1)
        case Branch(children, box) =>

          children.foreach { c =>
            recur(c)
          }
          children.drop(children.size - 1)
      }
    }

      recur(this.root)
  }

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def searchPoint(pt: Point): Entry[V] =
  {
    val ret=root.search(pt.toBox, _ => true)

    if(ret!=null&&ret.length!=0)
      ret.head
    else
      null

  }


  /**
   * Return a sequence of all entries found in the given search space.
   */
  def search(space: Box, f: Entry[V] => Boolean): Seq[Entry[V]] =
    root.search(space, f)



  /**
   * Return a sequence of all entries intersecting the given search space.
   */
  def searchIntersection(space: Box): Seq[Entry[V]] =
    root.searchIntersection(space, _ => true)

  /**
   * Return a sequence of all entries intersecting the given search space.
   */
  def searchIntersection(space: Box, f: Entry[V] => Boolean): Seq[Entry[V]] =
    root.searchIntersection(space, f)

  /**
   * Construct a result an initial value, the entries found in a
   * search space, and a binary function `f`.
   *
   *   rtree.foldSearch(space, init)(f)
   *
   * is equivalent to (but more efficient than):
   *
   *   rtree.search(space).foldLeft(init)(f)
   */
  def foldSearch[B](space: Box, init: B)(f: (B, Entry[V]) => B): B =
    root.foldSearch(space, init)(f)

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearest(pt: Point): Option[Entry[V]] =
    root.nearest(pt, Float.PositiveInfinity).map(_._2)

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearestK(pt: Point, k: Int): IndexedSeq[Entry[V]] =
    if (k < 1) {
      Vector.empty
    } else {
      implicit val ord = Ordering.by[(Double, Entry[V]), Double](_._1)
      val pq = PriorityQueue.empty[(Double, Entry[V])]
      root.nearestK(pt, k, Double.PositiveInfinity, pq)
      val arr = new Array[Entry[V]](pq.size)
      var i = arr.length - 1
      while (i >= 0) {
        val (_, e) = pq.dequeue
        arr(i) = e
        i -= 1
      }
      arr
    }

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearestK(pt: Point,k: Int,z:Entry[V]=>Boolean): IndexedSeq[(Double,Entry[V])] = {
    if (k < 1) {
      Vector.empty
    } else {
      implicit val ord = Ordering.by[(Double, Entry[V]), Double](_._1)
      val pq = PriorityQueue.empty[(Double, Entry[V])]
      root.nearestK(pt, k, Double.PositiveInfinity, z, pq)

     // pq.toList
      val arr = new Array[(Double, Entry[V])](pq.size)
      var i = arr.length - 1
      while (i >= 0) {
        val (d, e) = pq.dequeue
        arr(i) =(d,e)
        i -= 1
      }
      arr
    }
  }


  /*
  def knnjoin[K: ClassTag]
  (stree:RTree[V], k:Int)
  (f1:(K)=>Boolean, f2:(V)=>Boolean):
  Iterator[(K, Double, Iterator[(K,V)])] =
  {

    stree.sortInternalnode()
    this.sortInternalnode()

    val buf = mutable.HashMap.empty[Geom, mutable.PriorityQueue[(Double,Entry[V])]]

    //return the max knn distance bound
    def updatePQ(key:Geom, datapoint:Entry[V]):Double=
    {
      try {

        if(buf.contains(key))
        {
          val pq=buf.get(key).get

          val d = datapoint.geom.distance(key.asInstanceOf[Point])

          //update the priority queue for this query point
          if(f1(datapoint.geom.asInstanceOf[K])&&f2(datapoint.value))
          {
            if(pq.size==k)
            {
              var maxdist = pq.head._1
              if (d < maxdist) {
                pq += ((d, (datapoint)))
                if (pq.size > k) {
                  pq.dequeue
                  maxdist = pq.head._1
                }
              }
            }else
            {
              pq += ((d, (datapoint)))
            }
          }

          buf.put(key,pq)

          if(pq.size==k) {
            pq.head._1
          }else
          {
            Double.MaxValue
          }

        }else
        {
          implicit val ord = Ordering.by[(Double, Entry[V]), Double](_._1)
          val pq = PriorityQueue.empty[(Double, Entry[V])]
          val distance = datapoint.geom.distance(key.asInstanceOf[Point])
          pq += ((distance, datapoint))
          buf.put(key,pq)

          if(pq.size==k) {
            distance
          }else
          {
            Double.MaxValue
          }
        }

      }catch
        {
          case e:Exception=>
           println("update heap exception"+e)
            0
        }
    }

    def getboundforPoint(key:Geom):Double=
    {
      var maxdist= Double.MaxValue
      if(buf.contains(key))
      {
        val pq=buf.get(key).get
        if(pq.size==k)
          maxdist= pq.head._1
      }

      maxdist

    }


    //case 3: the data tree is leaf, but the query tree is branch
    //the entry is data, and node is the query point
    //if the return is the maxvalue, then, it means those query point for this branch do not find their correct knn
    def recur(querybranch: Node[V], datapoints:Vector[Entry[V]], dataleafbox:Box): Double =
    {
      querybranch match {
        case Leaf(children, box) =>

          var distance=Double.MinValue
          //childern is those query point
          children.foreach {
            c => datapoints.foreach
                {
                  dpt=> distance=Math.max(distance,updatePQ(c.geom,dpt))
                }
          }

          children.foreach {
            c =>
              if(buf.contains(c.geom)&&buf.get(c.geom).get.size!=k)
                return Double.MaxValue
          }

          distance

        case Branch(children, box) =>
          //val cs = children.map(node => (node.box.distance(entry.geom.asInstanceOf[Point]), node)).sortBy(_._1)
          var maxdistance=0.0
          children.foreach
          {
              child=>
                if(child.box.mindistance(dataleafbox)<child.knnboundary)
                {
                  val tmpknnbound=recur(child,datapoints,dataleafbox)
                  maxdistance=Math.max(maxdistance,tmpknnbound)
                  child.updatebound(tmpknnbound)
                }
          }
          maxdistance
      }
    }


    //recursive to the leaf node, which contain the data itself
    //case 4: the data tree is branch, but the query tree is leaf
    def recur2(rootnode: Node[V], querypoint:Point): Double ={

        if(buf.contains(querypoint))
        {
          buf.get(querypoint).get match
          {
            case pq:PriorityQueue[(Double, Entry[V])] =>
              val maxdistance=rootnode.nearestK(querypoint, k, Double.PositiveInfinity, pq)
              if(pq.size==k) maxdistance else Double.MaxValue
          }

        }else
        {
          implicit val ord = Ordering.by[(Double, Entry[V]), Double](_._1)
          val pq = PriorityQueue.empty[(Double, Entry[V])]
          val maxdisance=rootnode.nearestK(querypoint, k, Double.PositiveInfinity, pq)
          buf.put(querypoint,pq)
          if(pq.size==k) maxdisance else Double.MaxValue
        }

      }


    //this is used for spatial knn join
    //return the knn distance bound from snode to rnode
    //rnode is the data node, snode is the query knn point node
    def knnjoin(rnode:Node[V],snode:Node[V]):Double={

      if(snode.box.mindistance(rnode.box)>snode.knnboundary)
      {
        //println("%"*100)
        return snode.knnboundary
      }

      val intesectionbox=rnode.box.intesectBox(snode.box)

        rnode match {
          case Leaf(children_r, box_r) =>
            snode match
            {
              case Leaf(children_s, box_s) => //case 2: both tree reach the leaf node

                var knnmaxboundFromStoR=0.0

                children_s.foreach
                {
                  case schild=>
                    var tmpbound=Double.MaxValue
                    children_r.foreach
                    {
                      case rchild=>
                        tmpbound=updatePQ(schild.geom,rchild)
                    }

                    knnmaxboundFromStoR=Math.max(tmpbound,knnmaxboundFromStoR)
                }

                snode.updatebound(knnmaxboundFromStoR)
                knnmaxboundFromStoR

              case Branch(children_s, box_s)=> //case 3: the data tree is leaf, but the query tree is branch
              {
                val knndistancebound=recur(snode,children_r,box_r)

                snode.updatebound(knndistancebound)
                knndistancebound
              }
            }

          case Branch(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 4: the data tree is branch, but the query tree is leaf

                var maxdistancebound=0.0

                //maxdistancebound=recur3(rnode,children_s,box_s)
                //snode.updatebound(maxdistancebound)

                children_s.foreach
                {
                  querypoint=>
                    maxdistancebound=Math.max(maxdistancebound,recur2(rnode,querypoint.geom.asInstanceOf[Point]))
                }
                snode.updatebound(maxdistancebound)
                maxdistancebound

              case Branch(children_s, box_s)=> //case 1: both of two tree are branch
              {

                //println("case 1 for two branches")
                children_r.sortBy{case(e)=>e.geom.x2}

                val rcopy=ArrayBuffer.empty[(Node[V],Int)]
                val rcopy2=ArrayBuffer.empty[(Node[V],Int)]

                for(i<-0 to children_r.length-1)
                {
                  rcopy.append((children_r(i),i))
                  rcopy2.append((children_r(i),i))
                }
                //rcopy is sorted by the x2 location
                rcopy2.sortBy{case(e,i)=>e.geom.x}

                val rmap=new mutable.HashMap[Node[V], ArrayBuffer[(Double, Node[V])]]

                def internalloop(entry:Node[V], marked:Int, label:Boolean, s:Vector[Node[V]]): Unit =
                {
                  //the entry is the r node
                    var i=marked
                    while(i<s.size&&s(i).geom.x<=entry.geom.x2)
                    {
                      //if there is overlap
                      if(entry.geom.y<=s(i).geom.y2&&entry.geom.y2>=s(i).geom.y)
                      {
                        knnjoin(entry,s(i))
                      }else
                      //not overlap
                      {
                        //postpone the knn join for those pairs
                        //store the min distance for s(i)
                        //do this knn later
                        if(rmap.contains(s(i)))
                        {
                          val a=rmap.get(s(i)).get
                          a.append((s(i).box.mindistance(entry.box),entry))
                          rmap.put(s(i),a)
                        }else
                        {
                          val a=ArrayBuffer.empty[(Double,Node[V])]
                          a.append((s(i).box.mindistance(entry.box),entry))
                          rmap.put(s(i),a)
                        }
                      }
                      i+=1
                    }

                }

                //entry is the query, and s are the data nodes
                def internalloopForS(entry:Node[V], marked:Int, label:Boolean, s:Array[(Node[V],Int)]): Unit =
                {
                  //entry is the snode
                    var i=marked
                    while(i<s.size&&s(i)._1.geom.x<=entry.geom.x2)
                    {
                      if(entry.geom.y<=s(i)._1.geom.y2&&entry.geom.y2>=s(i)._1.geom.y)
                      {
                        knnjoin(entry,s(i)._1)
                      }else
                      //not overlap
                      {
                        //postpone the knn join for those pairs
                        //store the min distance for s(i)
                        //do this knn later
                        if(rmap.contains(entry))
                        {
                          val a=rmap.get(entry).get
                          a.append((entry.box.mindistance(s(i)._1.box),s(i)._1))
                          rmap.put(entry,a)

                        }else
                        {
                          val a=ArrayBuffer.empty[(Double,Node[V])]
                          a.append((entry.box.mindistance(s(i)._1.box),s(i)._1))
                          rmap.put(entry,a)
                        }

                      }
                      i+=1
                    }

                  var j=i

                    //search for the left part
                  if(i<s.size)
                  {
                    while(i<s.size&&(entry.box.mindistance(s(i)._1.box))<=entry.knnboundary)
                    {
                      knnjoin(s(i)._1,entry)
                      i+=1
                    }
                  }

                    //search for the right part
                  if(j<s.size&&j>=0)
                  {
                    val index=s(j)._2
                    j=rcopy(index)._2
                    while(j>=0&&(entry.box.mindistance(rcopy(j)._1.box))<=entry.knnboundary)
                    {
                      knnjoin(rcopy(j)._1,entry)
                      j=j-1
                    }
                  }
                  //search for the postpone part

                  val o=rmap.get(entry).getOrElse(None)

                  if (o.isInstanceOf[ArrayBuffer[_]])
                  { // type check

                    val rest = o.asInstanceOf[ArrayBuffer[(Double,Node[V])]] // cast
                    rest.sortBy(_._1)
                    j=0
                    while(j<rest.size&&rest(j)._1<=entry.knnboundary)
                    {
                      rest(j)._2 match
                      {
                        case datanode:Node[V]=>
                          knnjoin(datanode,entry)
                          j=j+1
                      }

                    }
                  }

                }

                var i=0
                var j=0

                val r=rcopy2
                val s=children_s
                //this cost is O(r)+O(s)+O(inter)+O(bounary)

                while(i<r.size&&j<s.size)
                {
                  if(r(i)._1.geom.x<s(j).geom.x)
                  {
                    internalloop(r(i)._1,j,true,s)
                    i+=1
                  }else
                  {
                    internalloopForS(s(j),i,false,r.toArray)
                    j+=1
                  }
                }


                var maxvalue=0.0
                children_s.foreach
                {
                  s=> {
                    maxvalue=Math.max(maxvalue,s.knnboundary)
                    if(maxvalue==Double.MaxValue)
                    {
                      snode.updatebound(maxvalue)
                      return maxvalue
                    }

                  }
                }

                snode.updatebound(maxvalue)
                maxvalue
              }
            }
        }
      }


    //run the knn join betwee data and query
    knnjoin(this.root,stree.root)

    stree.cleanTree()
    //println(" case4times" +case4times)

    buf.map{
      case(geom,pq)=>
        val arr = new Array[(K,V)](pq.size)
        var i = arr.length - 1
        val maxdistance=pq.head._1
        while (i >= 0) {
          val (_, e) = pq.dequeue
          arr(i) = (e.geom.asInstanceOf[K],e.value)
          i -= 1
        }
        (geom.asInstanceOf[K],maxdistance,arr.toIterator)
    }.toIterator

  }*/

  /**
  //recursive to the leaf node, which contain the data itself
    //case 4: the data tree is branch, but the query tree is leaf
    def recur3(rootnode: Node[V], querypoints:Vector[Entry[V]], leafbox:Box): Double ={

      var knnbound=Double.MaxValue

      rootnode match {
        case Leaf(children, box) =>
          //leaf vs leaf condition
              querypoints.foreach
              {
                case querychild=>
                  var tmpbound=0.0
                  children.foreach
                  {
                    case datachild =>
                      tmpbound=updatePQ(querychild.geom,datachild)
                  }

                  knnbound=Math.max(tmpbound,knnbound)
              }

        case Branch(children, box) =>

          val cs = children.map{node =>(node.box.mindistance(leafbox),node)}.sortBy(_._1)

          cs.foreach {
            case (d, node) =>
            if (d >knnbound) {
              return knnbound //scalastyle:ignore
            }
              //use the point to box distance to check this bound

              val tmppoints=ArrayBuffer.empty[Entry[V]]
              var label=true
              for(i <- 0 to querypoints.size-1 )
              {
                val knndistance=getboundforPoint(querypoints(i).geom)
                if(knndistance>node.box.distance(querypoints(i).geom.asInstanceOf[Point]))
                {
                  label=false
                  tmppoints.append(querypoints(i))
                }
              }

              if(label==false)
                knnbound =Math.min(knnbound,recur3(node,tmppoints.toVector,leafbox))

              /*if(d!=0)
              {

              }else
              {
                knnbound =Math.min(knnbound,recur3(node,querypoints,leafbox))

              }*/

              //iterate+=1
          }

          //println("iterate "+iterate)
      }

      knnbound

      }

   */

  /*val r=children_r.filter(entry=>entry.geom.intersects(intesectionbox))
                 val s=children_s.filter(entry=>entry.geom.intersects(intesectionbox))

                 val nonr=children_r.filter(entry=> !(entry.geom.intersects(intesectionbox)))
                 val nons=children_s.filter(entry=> !(entry.geom.intersects(intesectionbox)))

                 s.foreach
                 {
                   case schild=>
                     var tmpbound=getboundforPoint(schild.geom)
                     r.foreach
                     {
                       case rchild=>
                         val distance=schild.geom.distance(rchild.geom.asInstanceOf[Point])
                         if(distance<=tmpbound)
                         {
                           tmpbound=updatePQ(schild.geom,rchild)
                         }
                     }
                 }


               s.foreach
               {
                 case schild=>
                   var tmpbound=getboundforPoint(schild.geom)
                   nonr.foreach
                   {
                     case rchild=>
                       val distance=schild.geom.distance(rchild.geom.asInstanceOf[Point])

                       if(distance<=tmpbound)
                       {
                         tmpbound=updatePQ(schild.geom,rchild)
                       }
                   }
               }

               nons.foreach
               {
                 case schild=>
                   var tmpbound=getboundforPoint(schild.geom)
                   r.foreach
                   {
                     case rchild=>
                       val distance=schild.geom.distance(rchild.geom.asInstanceOf[Point])
                       if(distance<=tmpbound)
                       {
                         tmpbound=updatePQ(schild.geom,rchild)
                       }
                   }
               }

                 nons.foreach
                 {
                   case schild=>
                     var tmpbound=getboundforPoint(schild.geom)
                     nonr.foreach
                     {
                       case rchild=>
                         val distance=schild.geom.distance(rchild.geom.asInstanceOf[Point])
                         if(distance<=tmpbound)
                         {
                           tmpbound=updatePQ(schild.geom,rchild)
                         }
                     }
                 }
                 */

  /*children_s.foreach
  {
    entry=>
      val bound=getboundforPoint(entry.geom)
      if(bound==Double.MaxValue)
      {
        snode.updatebound(bound)
        return bound
      }else
      {
        knnmaxboundFromStoR=Math.max(knnmaxboundFromStoR,bound)
      }

  }*/
  //println("knnmaxboundFromStoR "+knnmaxboundFromStoR)

  /**
  //the approach proposed in previous work
              val (inter1,nonter1)=intersection_knn(children_r,children_s)

                //val map=mutable.HashMap.empty[Node[V],Double]
                // for intersect part
                inter1.foreach
                {
                  case (overlap, rc,sc)=>
                      val knnBoundFromRCtoSC=knnjoin(rc,sc)
                     sc.updatebound(knnBoundFromRCtoSC)
                }

                //for those non intersection parts
                nonter1.groupBy(x=>x._3).foreach
                {
                  case (key,array)=>
                    array.sortBy(_._1).foreach
                    {
                      case(mindistance,rc,sc)=>

                        if(mindistance<sc.knnboundary)
                        {
                          val knnBoundFromRCtoSC=knnjoin(rc,sc)
                          sc.updatebound(knnBoundFromRCtoSC)
                        }
                    }
                }

                var maxvalue=Double.MinValue
                children_s.foreach
                {
                 s=> {
                      maxvalue=Math.max(maxvalue,s.knnboundary)
                      if(maxvalue==Double.MaxValue)
                        return maxvalue
                    }
                }

                //if all those subnode find its related knn pairs, return the knn max distancebound
                //or else return the double.maxvalue
                snode.updatebound(maxvalue)
                maxvalue
   */

  /**
   * Return a count of all entries found in the given search space.
   */
  def count(space: Box): Int =
    root.count(space)

  /**
   * Return whether or not the value exists in the tree at (x, y).
   */
  def contains(x: Float, y: Float, value: V): Boolean =
    root.contains(Entry(Point(x, y), value))

  /**
   * Return whether or not the given entry exists in the tree.
   */
  def contains(entry: Entry[V]): Boolean =
    root.contains(entry)

  /**
   * Return whether or not the given entry exists in the tree.
   */
  def contains[K](k:K, value:V): Boolean =
  {
    root.contains(Entry(k.asInstanceOf[Point], value) )
  }


  /**
   * Map the entry values from A to B.
   */
  def map[B](f: V => B): RTree[B] =
    RTree(root.map(f), size)

  /**
   * Return an iterator over all entries in the tree.
   */
  def entries: Iterator[Entry[V]] =
    root.iterator

  /**
   * Return an iterator over all values in the tree.
   */
  def values: Iterator[V] =
    entries.map(_.value)

  /**
   * Return a nice depiction of the tree.
   *
   * This method should only be called on small-ish trees! It will
   * print one line for every branch, leaf, and entry, so for a tree
   * with thousands of entries this will result in a very large
   * string!
   */
  def pretty: String = root.pretty

  override def toString: String =
    s"RTree(<$size entries>)"
}
