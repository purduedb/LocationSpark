# LocationSpark

We present LocationSpark, a spatial data processing system built on top of Apache Spark, a widely
used distributed data processing system. LocationSpark offers a rich set of spatial query operators 
such as range search, $k$NN, spatio-textual operation, spatial-join, and $k$NN-join. To achieve high performance, LocationSpark employs various spatial indexes for in-memory data, and guarantees that immutable spatial indexes have low overhead with fault tolerance. In addition, we build two new layers over Spark, namely a query scheduler and a query executor. 
The query scheduler is responsible for mitigating skew in spatial queries, while the query executor selects the best plan 
based on the indexes and the nature of the spatial queries.Furthermore, to avoid unnecessary network communication overhead when processing overlapped spatial data, an efficient spatial bloom filter is embedded into the indexes of LocationSpark. Finally, LocationSpark tracks frequently accessed spatial data, and dynamically flushes less frequently accessed data into disk.

## Spatial operators 
###Update RDD, Range Query, KNN Query, Spatial Textual Query, Spatial Join, kNN Join
```scala
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialindex.rtree._

    val conf = new SparkConf().setAppName("Spark SpatialRDD").setMaster("local[2]")

    val spark = new SparkContext(conf)
    val numofpartition=9
    /**
     * build a spatialrdd
     */
    val rdd = spark.parallelize((1 to 100000).map(x => (Util.uniformPoint(1000,1000), x)), numofpartition)
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
    *spatial join
    */
    val boxes=Array((Box(-21,30,-35,35)),(Box(-28.111,81.333,-31.33,86.333)))
    val queryBoxes=spark.parallelize(boxes,numofpartition)
    val joinresultRdd=indexed.sjoin(transfromQueryRDD)((k,id)=>id)
    
 
    
    
    
....
