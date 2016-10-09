/**
  * Created by Shen on 2016/9/13.
  *
  *usage: [data][numClusters][numIterations]
  * example: $ spark-submit --class bench.mllib.swt.KMeansTest  \
  *                         --master yarn     \
  *                         --deploy-mode cluster      \
  *                         --queue default  \
  *                         /usr/local/hadoop/shen/SparkFaultBench.jar \
  *                         data/swt/retail.txt \
  *                         18 10 random
  */

package bench.mllib.swt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vectors}

object KMeansTest {

  def main(args: Array[String]) {

    val conf = new SparkConf()
        .setAppName("KMeansTest")

    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0))
    val parsedData = data.map(s => Vectors.dense(s.replaceAll("\\[|\\]","").split(",").map(_.toDouble))).cache()
    val numClusters = if (args.length > 1) args(1).toInt else 2
    val numIterations = if (args.length > 2) args(2).toInt else 20
    val initializedMode = if (args.length > 3) args(3) else "k-means||"

    var clusterIndex = 0
    val clusters = KMeans.train(parsedData, numClusters, numIterations, 1, initializedMode)
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")

    clusters.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })

    sc.stop()
  }
}