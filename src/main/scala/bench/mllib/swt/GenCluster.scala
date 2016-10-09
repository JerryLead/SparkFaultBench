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


import org.apache.spark.mllib.linalg.Vectors
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


      distrib match {
        case "normal" =>
          val pairs = {
            var seqNormal: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes) {
              var ins = 0
              val pair = RandomRDDs.normalRDD(sc, instances, partions)
                .mapPartitionsWithIndex((x, iter) =>{
                  iter.map(i => {
                    ins += 1
                    (x * (instances) + ins, i)
                  }
                    )
                })
              seqNormal = seqNormal :+ pair
            }
            sc.union(seqNormal).groupByKey(partions).values.map(x => Vectors.dense(x.toArray))
          }.cache()
          pairs.saveAsTextFile(path)

        case "gamma" =>
          val pairs = {
            var seqGamma: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes) {
              var ins = 0
              val pair = RandomRDDs.gammaRDD(sc, 9, 0.5, instances, partions)
                .mapPartitionsWithIndex((x, iter) =>{
                iter.map(i => {
                  ins += 1
                  (x * (instances) + ins, i)
                }
                )
              })
              seqGamma = seqGamma :+ pair
            }
            sc.union(seqGamma).groupByKey(partions).values.map(x => Vectors.dense(x.toArray))
          }.cache()
          pairs.saveAsTextFile(path)

        case "poisson" =>
          val pairs = {
            var seqPoisson: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes) {
              var ins = 0
              val pair = RandomRDDs.poissonRDD(sc, 1, instances, partions)
                .mapPartitionsWithIndex((x, iter) =>{
                  iter.map(i => {
                    ins += 1
                    (x * (instances) + ins, i)
                  }
                  )
                })
              seqPoisson = seqPoisson :+ pair
            }
            sc.union(seqPoisson).groupByKey(partions).values.map(x => Vectors.dense(x.toArray))
          }.cache()
          pairs.saveAsTextFile(path)

        case "exponential" =>
          val pairs = {
            var seqExponential: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes) {
              var ins = 0
              val pair = RandomRDDs.exponentialRDD(sc, 1, instances, partions)
                .mapPartitionsWithIndex((x, iter) =>{
                  iter.map(i => {
                    ins += 1
                    (x * (instances) + ins, i)
                  }
                  )
                })
              seqExponential = seqExponential :+ pair
            }
            sc.union(seqExponential).groupByKey(partions).values.map(x => Vectors.dense(x.toArray))
          }.cache()
          pairs.saveAsTextFile(path)

        case "uniform" =>
          val pairs = {
            var seqUniform: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes) {
              var ins = 0
              val pair = RandomRDDs.uniformRDD(sc, instances, partions)
                .mapPartitionsWithIndex((x, iter) =>{
                  iter.map(i => {
                    ins += 1
                    (x * (instances) + ins, i)
                  }
                  )
                })
              seqUniform = seqUniform:+pair
            }
            sc.union(seqUniform).groupByKey(partions).values.map(x => Vectors.dense(x.toArray))
          }.cache()
          pairs.saveAsTextFile(path)

        case "mix" =>
          val pairs = {
            var seqUniform: Seq[RDD[(Int, Double)]] = Seq()
            for (attr <- 0 until attributes/4 ){
              var ins1 = 0
              val pair1 = RandomRDDs.uniformRDD(sc, instances, partions)
                .mapPartitionsWithIndex((x, iter) =>{
                  iter.map(i => {
                    ins1 += 1
                    (x * (instances) + ins1, i)
                  }
                  )
                })
              var ins2 = 0
              val pair2 = RandomRDDs.poissonRDD(sc, 1, instances, partions)
                .mapPartitionsWithIndex((x, iter) =>{
                  iter.map(i => {
                    ins2 += 1
                    (x * (instances) + ins2, i)
                  }
                  )
                })
              var ins3 = 0
              val pair3 = RandomRDDs.gammaRDD(sc, 9, 0.5, instances, partions)
                .mapPartitionsWithIndex((x, iter) =>{
                  iter.map(i => {
                    ins3 += 1
                    (x * (instances) + ins3, i)
                  }
                  )
                })
              var ins4 = 0
              val pair4 = RandomRDDs.exponentialRDD(sc, 1, instances, partions)
                .mapPartitionsWithIndex((x, iter) =>{
                  iter.map(i => {
                    ins4 += 1
                    (x * (instances) + ins4, i)
                  }
                  )
                })
              seqUniform = seqUniform:+pair1:+pair2:+pair3:+pair4
            }
            for (i <- 0 until attributes % 4 ){
              var ins = 0
              val pair = RandomRDDs.normalRDD(sc, instances, partions)
                .mapPartitionsWithIndex((x, iter) =>{
                  iter.map(i => {
                    ins += 1
                    (x * (instances) + ins, i)
                  }
                  )
                })
              seqUniform = seqUniform:+pair
            }
            sc.union(seqUniform).groupByKey(partions).values.map(x => Vectors.dense(x.toArray))
          }.cache()
          pairs.saveAsTextFile(path)


        case  _ =>
          val pairs = RandomRDDs.uniformVectorRDD(sc, instances, attributes, partions)
          pairs.saveAsTextFile(path)

      }
    sc.stop()
  }
}
