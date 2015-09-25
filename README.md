# SpatialSpark
The spatial spark based on the spatial index
The spatial index is based on the immutable quadtree and R-tree inside the RDD

## Spatial operators 
###Range query and Knn query 
```scala
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialindex.rtree._

    val conf = new SparkConf().setAppName("Spark SpatialRDD").setMaster("local[2]")

    val spark = new SparkContext(conf)

    /**
     * test for building a spatialrdd
     */
    val rdd = spark.parallelize((1 to 100000).map(x => (Util.uniformPoint(1000,1000), x)), 9)
    val indexed = SpatialRDD(rdd).cache()

    /**
     * test for put and get function
     */
    val insertpoint=Point(300, 383)
    val indexrdd2=indexed.put(insertpoint,100)
    assert(indexrdd2.get(insertpoint)==Some(100))
    val insertpoint2=Point(-17, 18)
    assert(indexrdd2.get(insertpoint2)==None)

    /**
     * test for multi-put and multi-get
     */
    var z = Array(Point(12,12), Point(17,18), Point(20,21))
    val indexrdd3=indexrdd2.multiput(Map(Point(12,12) -> 1, Point(17,18) -> 12, Point(20,21)->100), SumFunction)
    indexrdd3.multiget(z).foreach(println)

    /**
     * test for range query
     */
    val box = Box(2 , 2, 90, 90)
    val rangesearchresult=indexrdd3.rangeFilter(box,(id)=>true)
    //rangesearchresult.foreach(println)

    /**
     * test for knn search
     */
    val knnresults=indexrdd3.knnFilter(insertpoint,200,(id)=>true)
```
....
