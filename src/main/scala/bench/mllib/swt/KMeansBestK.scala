package bench.mllib.swt

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by Shen on 2016/9/14.
  * usage: [data] [itermin] [itermax]
  * example: $ spark-submit --class bench.mllib.swt.KMeansBestK  \
  *                         --master yarn     \
  *                         --deploy-mode cluster      \
  *                         --queue default  \
  *                         /usr/local/hadoop/shen/SparkFaultBench.jar \
  *                         data/swt/retail.txt \
  *                         3 10 random
  */
object KMeansBestK {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("KMeansBestK")
//      .setMaster("local[2]")
//      .set("spark.sql.warehouse.dir", "file:///E:/Shen/spark-warehouse")
    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0))
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    val itermin = args(1).toInt
    val itermax = args(2).toInt
    val initializedMode = args(3)

    val ks: List[Int] = List.range(itermin,itermax)
    ks.foreach(cluster => {
      val clusters = KMeans.train(parsedData, cluster, 100, 1, initializedMode)
      val WSSSE = clusters.computeCost(parsedData)
      println("sum of squared distances of points to their nearest center when k=" + cluster + " -> " + WSSSE)
    })

    sc.stop()
  }
}
