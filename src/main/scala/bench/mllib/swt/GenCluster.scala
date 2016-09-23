package bench.mllib.swt

/**
  * Created by Shen on 2016/9/21.
  * Genarate data for cluster.
  * Parameters: [attributes] columns
  *             [instances]  rows
  *             [distrib]    form of distribution
  *             [partions]   partions
  *             [path]       path of data to write
  */

//import java.util.Random

import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.{SparkConf, SparkContext}

object GenCluster {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("GenCluster")
    val sc = new SparkContext(conf)

    val time = new java.util.Date
    val attributes = if (args.length > 0) args(0).toInt else 3
    val instances = if (args.length > 1) args(1).toInt else 100
    val distrib = if (args.length > 2) args(2).toString else "normal"
    val partions = if (args.length > 3) args(3).toInt else 1

    val path = if (args.length > 4) args(4) else "data/swt/cluster" + distrib + time.getTime()

//    val path = "file:///E:/Shen/SparkFaultTolerant/DataSource/cluster"+ distrib + time.getTime()


    var  i = 0
      distrib match {
        case "random" =>
          val pairs = {
            var pair = RandomRDDs.normalRDD(sc, instances, partions).map(x => {
              i += 1
              (i, x)
            })
            for (attr <- 1 until attributes) {
              var ins = 0
              pair = RandomRDDs.normalRDD(sc, instances, partions).map(x => {
                ins += 1
                (ins, x)
              }).union(pair)
            }
            pair.groupByKey(partions).values
          }.cache()
//          val pairs = RandomRDDs.normalVectorRDD(sc, instances, attributes, partions)
          pairs.saveAsTextFile(path)

        case "gamma" =>
          val pairs = {
            var pair = RandomRDDs.gammaRDD(sc, 9, 0.5, instances, partions).map(x => {
              i += 1
              (i, x)
            })
            for (attr <- 1 until attributes) {
              var ins = 0
              pair = RandomRDDs.gammaRDD(sc, 9, 0.5, instances, partions).map(x => {
                ins += 1
                (ins, x)
              }).union(pair)
            }
            pair.groupByKey(partions).values
          }.cache()
//          val pairs = RandomRDDs.gammaVectorRDD(sc, 9, 0.5, instances, attributes, partions)
          pairs.saveAsTextFile(path)

        case "poisson" =>
          val pairs = {
            var pair = RandomRDDs.poissonRDD(sc, 1, instances, partions).map(x => {
              i += 1
              (i, x)
            })
            for (attr <- 1 until attributes) {
              var ins = 0
              pair = RandomRDDs.poissonRDD(sc, 1, instances, partions).map(x => {
                ins += 1
                (ins, x)
              }).union(pair)
            }
            pair.groupByKey(partions).values
          }.cache()
//          val pairs = RandomRDDs.poissonVectorRDD(sc, 1, instances, attributes, partions)
          pairs.saveAsTextFile(path)

        case "exponential" =>
          val pairs = {
            var pair = RandomRDDs.exponentialRDD(sc, 1, instances, partions).map(x => {
              i += 1
              (i, x)
            })
            for (attr <- 1 until attributes) {
              var ins = 0
              pair = RandomRDDs.exponentialRDD(sc, 1, instances, partions).map(x => {
                ins += 1
                (ins, x)
              }).union(pair)
            }
            pair.groupByKey(partions).values
          }.cache()
//          val pairs = RandomRDDs.exponentialVectorRDD(sc, 1, instances, attributes, partions)
          pairs.saveAsTextFile(path)

        case "uniform" =>
          val pairs = {
            var pair = RandomRDDs.uniformRDD(sc, instances, partions).map(x => {
              i += 1
              (i, x)
            })
            for (attr <- 1 until attributes) {
              var ins = 0
              pair = RandomRDDs.uniformRDD(sc, instances, partions).map(x => {
                ins += 1
                (ins, x)
              }).union(pair)
            }
            pair.groupByKey(partions).values
          }.cache()
//          val pairs = RandomRDDs.uniformVectorRDD(sc, instances, attributes, partions)
          pairs.saveAsTextFile(path)


        case "normal" =>
          val pairs = RandomRDDs.uniformVectorRDD(sc, instances, attributes, partions)
          pairs.saveAsTextFile(path)

      }
    sc.stop()
  }
}
