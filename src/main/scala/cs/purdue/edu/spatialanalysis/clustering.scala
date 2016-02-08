package cs.purdue.edu.spatialanalysis

import cs.purdue.edu.spatialindex.rtree.Point

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Created by merlin on 1/17/16.
 */
class clustering {

}

/**
 * local kmeans algorithm
 */
class Kmeans[T](
                 points: IndexedSeq[T],
                 distanceFun: (T,T)=>Double = Kmeans.euclideanDistance,
                 minChangeInDispersion: Double = 0.0001,
                 maxIterations: Int = 100,
                 fixedSeedForRandom: Boolean = false
                 )
{


  /**
   * Run the k-means algorithm on this set of points for some given k.
   *
   * @param k The number of clusters to produce.
   * @param restarts The number of times to run k-means from different random
   *     starting points.
   *
   * @return A pair, the first element of which is the dispersion for the best
   *     set of centroids found, and the second element of which is that set of
   *     centroids.
   */
  def run(k: Int, restarts: Int = 25): (Double, IndexedSeq[T]) = {
    val runResults = (1 to restarts).map(_ => moveCentroids(chooseRandomCentroids(k)))
    val (bestDispersion, bestCentroids) = runResults.minBy(_._1)
    (bestDispersion, bestCentroids)
  }

  /**
   * Run the k-means algorithm starting from the given set of centroids. This
   * is an iterative version since it runs faster than a nicer recursive one.
   *
   * @return A pair, the first element of which is the dispersion for the
   *     best set of centroids found, and the second element of which is that
   *     set of centroids.
   */
  private[this] def moveCentroids(centroids: IndexedSeq[T]): (Double, IndexedSeq[T]) = {
    val numClusters = centroids.length
    var iteration = 0
    var lastDispersion = Double.PositiveInfinity
    var dispersionChange = Double.PositiveInfinity
    var changingCentroids = centroids
    while (iteration < maxIterations && dispersionChange > minChangeInDispersion) {
      val (dispersion, memberships) = computeClusterMemberships(changingCentroids)
      changingCentroids = computeCentroids(memberships,numClusters)
      dispersionChange = math.abs(lastDispersion - dispersion)
      lastDispersion = dispersion
      iteration += 1
    }
    (lastDispersion, changingCentroids)
  }

  /**
   *  Given a sequence of centroids, compute the cluster memberships for each point.
   *
   *  @param centroids A sequence of points representing centroids.
   *  @return A pair, the first element of which is the dispersion given these centroids,
   *       and the second of which is the list of centroid indices for each of the points
   *       being clustered (based on the nearest centroid to each).
   */
  def computeClusterMemberships(centroids: IndexedSeq[T]) = {

    val (squaredDistances, memberships) = points.par.map {
      point =>
      val distances = centroids.map(c=>distanceFun(c,point))
      val (shortestDistance, closestCentroid) = distances.zipWithIndex.min
      (shortestDistance * shortestDistance, closestCentroid)
    }.toIndexedSeq.unzip

    (squaredDistances.sum, memberships)
  }

  /**
   * Given memberships for each point, compute the centroid for each cluster.
   */
  private[this] def computeCentroids(memberships: IndexedSeq[Int], numClusters: Int) = {

    val centroids = Array.fill(numClusters)(0.0,0.0,0)

    var index = 0
    while (index < points.length) {
      val clusterId = memberships(index)
      if (clusterId > -1&&clusterId<numClusters) {
        points(index) match
        {
          case p:Point=>
            centroids(clusterId) = (centroids(clusterId)._1+p.x,centroids(clusterId)._2+p.y,centroids(clusterId)._3+1)
        }
      }
      index += 1
    }

    centroids.map
    {
      case(c1,c2,count)=>
        Point((c1/count).toFloat,(c2/count).toFloat).asInstanceOf[T]
    }.toIndexedSeq
  }

  /**
   * Randomly choose k of the points as initial centroids.
   */
  private[this] def chooseRandomCentroids(k: Int) =
  {
    var iteration=k
    val pivots=ArrayBuffer.empty[T]
    while(iteration>0)
    {
      val index=Random.nextInt(points.size-1)
      pivots.append(points(index))
      iteration-=1
    }
    pivots.toIndexedSeq
  }



}

/**
 * A companion to hold distance functions.
 */
object Kmeans {
  /**
   * Compute euclidean distance (l2 norm).
   */
  val euclideanDistance = (a: Point, b: Point) => {
    a.distance(b)
  }

}

