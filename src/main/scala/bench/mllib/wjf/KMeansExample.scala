package bench.mllib.wjf

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wjf on 2016/8/20.
  */
object KMeansExample {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("KMeansExample")
//    conf.setMaster("local")
//    conf.set("spark.sql.warehouse.dir","file:///")
//    conf.set("spark.sql.warehouse.dir","file:///")
    val sc = new SparkContext(conf)


    val data = sc.textFile("hdfs://133.133.134.108:9000/user/hadoop/data/wjf/Kmeans_random_small.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20



    val clusters = KMeans.train(parsedData, numClusters, numIterations,10)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
      clusters.save(sc, "hdfs://133.133.134.108:9000/user/hadoop/data/wjf/KMeansExample/KMeansModelBig")
    val sameModel = KMeansModel.load(sc, "hdfs://133.133.134.108:9000/user/hadoop/data/wjf/KMeansExample/KMeansModelBig")
    // $example off$
    sameModel.clusterCenters.foreach(println)
    sc.stop()
  }
}
