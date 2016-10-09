package bench.mllib.swt

import java.util.Random

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Shen on 2016/9/21.
  *
  * Generate LivSVM format file for Regression..
  *
  * Parameters: [attributes]
  *             [instances]
  *             [distribution]
  *             [partions]
  *             [path]

  */
object GenClassify {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("GenClassify")
    val sc = new SparkContext(conf)

    val time = new java.util.Date
    val attributes = if (args.length > 0) args(0).toInt else 10
    val instances = if (args.length > 1) args(1).toInt else 100
    val distrib = if (args.length > 2) args(2).toString else "normal"
    val partions = if (args.length > 3) args(3).toInt else 4

    val path = if (args.length > 4) args(4) else "data/swt/Classify" + distrib + time.getTime()

//        val path = "file:///E:/Shen/SparkFaultTolerant/DataSource/Classify"+ distrib + time.getTime()
    val ranGen = new Random()
    distrib match {
      case "normal" =>
        val pairs = {
          var seqNormal: Seq[RDD[(Int, (Int, Double))]] = Seq()
          for (attr <- 0 until attributes) {
            var ins = 0
            val pair = RandomRDDs.normalRDD(sc, ranGen.nextInt(instances)+1, partions)
              .mapPartitionsWithIndex((x, iter) =>{
                iter.map(i => {
                  ins += 1
                  (x * instances + ins, (attr, i))
                }
                )
              })
            seqNormal = seqNormal :+ pair
          }
          sc.union(seqNormal).groupByKey(partions).values.map(x =>
            LabeledPoint(ranGen.nextInt(2),
              Vectors.sparse(attributes, x.toSeq)))
        }.cache()
        MLUtils.saveAsLibSVMFile(pairs, path)

      case "gamma" =>
        val pairs = {
          var seqGamma: Seq[RDD[(Int, (Int, Double))]] = Seq()
          for (attr <- 0 until attributes) {
            var ins = 0
            val pair = RandomRDDs.gammaRDD(sc, 9, 0.5, ranGen.nextInt(instances)+1, partions)
              .mapPartitionsWithIndex((x, iter) =>{
                iter.map(i => {
                  ins += 1
                  (x * instances + ins, (attr, i))
                }
                )
              })
            seqGamma = seqGamma :+ pair
          }
          sc.union(seqGamma).groupByKey(partions).values.map(x =>
            LabeledPoint(ranGen.nextInt(2),
              Vectors.sparse(attributes, x.toSeq)))
        }.cache()
        MLUtils.saveAsLibSVMFile(pairs, path)

      case "poisson" =>
        val pairs = {
          var seqPoi: Seq[RDD[(Int, (Int, Double))]] = Seq()
          for (attr <- 0 until attributes) {
            var ins = 0
            val pair = RandomRDDs.poissonRDD(sc, 1, ranGen.nextInt(instances)+1, partions)
              .mapPartitionsWithIndex((x, iter) =>{
              iter.map(i => {
                ins += 1
                (x * instances + ins, (attr, i))
              }
              )
            })
            seqPoi = seqPoi :+ pair
          }
          sc.union(seqPoi).groupByKey(partions).values.map(x =>
            LabeledPoint(ranGen.nextInt(2),
              Vectors.sparse(attributes, x.toSeq)))
        }.cache()
        MLUtils.saveAsLibSVMFile(pairs, path)

      case "exponential" =>
        val pairs = {
          var seqExp: Seq[RDD[(Int, (Int, Double))]] = Seq()
          for (attr <- 0 until attributes) {
            var ins = 0
            val pair = RandomRDDs.exponentialRDD(sc, 1, ranGen.nextInt(instances)+1, partions)
              .mapPartitionsWithIndex((x, iter) =>{
              iter.map(i => {
                ins += 1
                (x * instances + ins, (attr, i))
              }
              )
            })
            seqExp = seqExp :+ pair
          }
          sc.union(seqExp).groupByKey(partions).values.map(x =>
            LabeledPoint(ranGen.nextInt(2),
              Vectors.sparse(attributes, x.toSeq)))
        }.cache()
        MLUtils.saveAsLibSVMFile(pairs, path)

      case "uniform" =>
        val pairs = {
          var seqUniform: Seq[RDD[(Int, (Int, Double))]] = Seq()
          for (attr <- 0 until attributes) {
            var ins = 0
            val pair = RandomRDDs.uniformRDD(sc, ranGen.nextInt(instances)+1, partions)
              .mapPartitionsWithIndex((x, iter) =>{
                iter.map(i => {
                  ins += 1
                  (x * instances + ins, (attr, i))
                }
                )
              })
            seqUniform = seqUniform :+ pair
          }
          sc.union(seqUniform).groupByKey(partions).values.map(x =>
            LabeledPoint(ranGen.nextInt(2),
              Vectors.sparse(attributes, x.toSeq)))
        }.cache()
        MLUtils.saveAsLibSVMFile(pairs, path)

      case "mix" =>
        val pairs = {
          var seqMix: Seq[RDD[(Int, (Int, Double))]] = Seq()
          for (attr <- 0 until attributes/4 ){
            var ins1 = 0
            val pair1 = RandomRDDs.uniformRDD(sc, ranGen.nextInt(instances)+1, partions)
              .mapPartitionsWithIndex((x, iter) =>{
                iter.map(i => {
                  ins1 += 1
                  (x * instances + ins1, (4*attr, i))
                }
                )
              })
            var ins2 = 0
            val pair2 = RandomRDDs.poissonRDD(sc, 1, ranGen.nextInt(instances)+1, partions)
              .mapPartitionsWithIndex((x, iter) =>{
                iter.map(i => {
                  ins2 += 1
                  (x * instances + ins2, (4*attr+1, i))
                }
                )
              })
            var ins3 = 0
            val pair3 = RandomRDDs.gammaRDD(sc, 9, 0.5, ranGen.nextInt(instances)+1, partions)
              .mapPartitionsWithIndex((x, iter) =>{
                iter.map(i => {
                  ins3 += 1
                  (x * instances + ins3, (4*attr+2, i))
                }
                )
              })
            var ins4 = 0
            val pair4 = RandomRDDs.exponentialRDD(sc, 1, ranGen.nextInt(instances)+1, partions)
              .mapPartitionsWithIndex((x, iter) =>{
                iter.map(i => {
                  ins4 += 1
                  (x * instances + ins4, (4*attr+3, i))
                }
                )
              })
            seqMix = seqMix:+pair1:+pair2:+pair3:+pair4
          }
          for (attr <- 0 until attributes % 4 ){
            var ins = 0
            val pair = RandomRDDs.normalRDD(sc, ranGen.nextInt(instances)+1, partions)
              .mapPartitionsWithIndex((x, iter) =>{
                iter.map(i => {
                  ins += 1
                  (x * instances + ins, (attributes /4 *4 + attr, i))
                }
                )
              })
            seqMix = seqMix:+pair
          }
          sc.union(seqMix).groupByKey(partions).values.map(x =>
            LabeledPoint(ranGen.nextInt(2),
              Vectors.sparse(attributes, x.toSeq)))
        }.cache()
        MLUtils.saveAsLibSVMFile(pairs, path)

      case _ =>
        println(s"Wrong form of distribution.\n" +
          s"Distribution should be normal/ gamma/ poisson/ exponential/ uniform/ mix.")

    }
    sc.stop()
  }

}
