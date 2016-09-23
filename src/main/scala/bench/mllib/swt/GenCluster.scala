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
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GenCluster {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("GenCluster")
    val sc = new SparkContext(conf)

    val time = new java.util.Date
    val attributes = if (args.length > 0) args(0).toInt else 3
    val instances = if (args.length > 1) args(1).toInt else 100
    val distrib = if (args.length > 2) args(2).toString else "uniform"
    val partions = if (args.length > 3) args(3).toInt else 1

    val path = if (args.length > 4) args(4) else "data/swt/cluster" + distrib + time.getTime()

//    val path = "file:///E:/Shen/SparkFaultTolerant/DataSource/cluster"+ distrib + time.getTime()


//    var  i = 0
//    var seqPair = new Seq[RDD[String]](1)
      distrib match {
        case "random" =>
          val pairs = {
            var seqNormal: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes) {
              var ins = 0
              val pair = RandomRDDs.normalRDD(sc, instances, partions).map(x => {
                ins += 1
                (ins, x)
              })
              seqNormal = seqNormal :+ pair
            }
            sc.union(seqNormal).groupByKey(partions).values
          }.cache()
//          val pairs = RandomRDDs.normalVectorRDD(sc, instances, attributes, partions)
          pairs.saveAsTextFile(path)

        case "gamma" =>
          val pairs = {
            var seqGamma: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes) {
              var ins = 0
              val pair = RandomRDDs.gammaRDD(sc, 9, 0.5, instances, partions).map(x => {
                ins += 1
                (ins, x)
              })
              seqGamma = seqGamma :+ pair
            }
            sc.union(seqGamma).groupByKey(partions).values
          }.cache()
//          val pairs = RandomRDDs.gammaVectorRDD(sc, 9, 0.5, instances, attributes, partions)
          pairs.saveAsTextFile(path)

        case "poisson" =>
          val pairs = {
            var seqPoisson: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes) {
              var ins = 0
              val pair = RandomRDDs.poissonRDD(sc, 1, instances, partions).map(x => {
                ins += 1
                (ins, x)
              })
              seqPoisson = seqPoisson :+ pair
            }
            sc.union(seqPoisson).groupByKey(partions).values
          }.cache()
//          val pairs = RandomRDDs.poissonVectorRDD(sc, 1, instances, attributes, partions)
          pairs.saveAsTextFile(path)

        case "exponential" =>
          val pairs = {
            var seqExponential: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes) {
              var ins = 0
              val pair = RandomRDDs.exponentialRDD(sc, 1, instances, partions).map(x => {
                ins += 1
                (ins, x)
              })
              seqExponential = seqExponential :+ pair
            }
            sc.union(seqExponential).groupByKey(partions).values
          }.cache()
//          val pairs = RandomRDDs.exponentialVectorRDD(sc, 1, instances, attributes, partions)
          pairs.saveAsTextFile(path)

        case "uniform" =>
          val pairs = {
            var seqUniform: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes) {
              var ins = 0
              val pair = RandomRDDs.uniformRDD(sc, instances, partions).map(x => {
                ins += 1
                (ins, x)
              })
              seqUniform = seqUniform:+pair
            }
            sc.union(seqUniform).groupByKey(partions).values
          }.cache()
//          val pairs = RandomRDDs.uniformVectorRDD(sc, instances, attributes, partions)
          pairs.saveAsTextFile(path)

        case "mix" =>
          val pairs = {
            var seqUniform: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes/4 ){
              var ins1 = 0
              val pair1 = RandomRDDs.uniformRDD(sc, instances, partions).map(x => {
                ins1 += 1
                (ins1, x)
              })
              var ins2 = 0
              val pair2 = RandomRDDs.poissonRDD(sc, 1, instances, partions).map(x => {
                ins2 += 1
                (ins2, x)
              })
              var ins3 = 0
              val pair3 = RandomRDDs.gammaRDD(sc, 9, 0.5, instances, partions).map(x => {
                ins3 += 1
                (ins3, x)
              })
              var ins4 = 0
              val pair4 = RandomRDDs.exponentialRDD(sc, 1, instances, partions).map(x => {
                ins4 += 1
                (ins4, x)
              })
              seqUniform = seqUniform:+pair1:+pair2:+pair3:+pair4
            }
            for (i <- 0 until attributes % 4 ){
              var ins = 0
              val pair = RandomRDDs.normalRDD(sc, instances, partions).map(x => {
                ins += 1
                (ins, x)
              })
              seqUniform = seqUniform:+pair
            }
            sc.union(seqUniform).groupByKey(partions).values
          }.cache()
          //          val pairs = RandomRDDs.uniformVectorRDD(sc, instances, attributes, partions)
          pairs.saveAsTextFile(path)


        case  _ =>
          val pairs = RandomRDDs.uniformVectorRDD(sc, instances, attributes, partions)
          pairs.saveAsTextFile(path)

      }
    sc.stop()
  }
}
