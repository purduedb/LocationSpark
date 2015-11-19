# SpatialRDD

An Effecient spatialRDD based on the spatial index i.e., immutable quadtree and R-tree

## Spatial operators 
###Update RDD, Range Query, KNN Query, Spatial Textual Query, Spatial Join
```scala
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialindex.rtree._

    val conf = new SparkConf().setAppName("Spark SpatialRDD").setMaster("local[2]")

    val spark = new SparkContext(conf)

    /**
     * build a spatialrdd
     */
    val rdd = spark.parallelize((1 to 100000).map(x => (Util.uniformPoint(1000,1000), x)), 9)
    val indexed = SpatialRDD(rdd).cache()

    /**
     * put and get function
     */
    val insertpoint=Point(300, 383)
    val indexrdd2=indexed.put(insertpoint,100)
    assert(indexrdd2.get(insertpoint)==Some(100))
    val insertpoint2=Point(-17, 18)
    assert(indexrdd2.get(insertpoint2)==None)

    /**
     * multi-put and multi-get
     */
    var z = Array(Point(12,12), Point(17,18), Point(20,21))
    val indexrdd3=indexrdd2.multiput(Map(Point(12,12) -> 1, Point(17,18) -> 12, Point(20,21)->100), SumFunction)
    indexrdd3.multiget(z).foreach(println)

    /**
     * range query
     */
    val box = Box(2 , 2, 90, 90)
    val rangesearchresult=indexrdd3.rangeFilter(box,(id)=>true)
    //rangesearchresult.foreach(println)

    /**
     * knn search
     */
    val k=200
    val knnresults=indexrdd3.knnFilter(insertpoint,k,(id)=>true)

    /**
     * spatial text search
     */
    def textcondition[V](z:Entry[V]):Boolean=
    {
         z.value match
         {
           case v:String =>
             val vl=v.toLowerCase()
             (vl.contains("apple")||vl.contains("google")||v.contains("bad"))
         }
    }
    val box = Box(23.10094f,-86.8612f, 32.41f, -85.222f)
    val spatialtextualresult=indexrdd3.rangeFilter(box,textcondition)
  
   /**
    *test for spatial range join
    */
    val boxpartitioner=new Grid2DPartitionerForBox(qtreeUtil.rangx,qtreeUtil.rangx,9)
    val boxes=Array{(Box(-10,-10,299,399),1);(Box(900,900,1000,1000),2)}
    val queryBoxes=spark.parallelize(boxes,9)
    val transfromQueryRDD=queryBoxes.flatMap{
      case(box:Box,id)=>
        boxpartitioner.getPartitionsForRangeQuery(box).map(p=>(p,box))
    }
    val joinresultRdd=indexed.sjoin(transfromQueryRDD)((k,id)=>id)
    
....
