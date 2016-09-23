package bench.mllib.swt

import java.util.Random

import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
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
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val time = new java.util.Date
    val attributes = if (args.length > 0) args(0).toInt else 10
    val instances = if (args.length > 1) args(1).toInt else 100
    val distrib = if (args.length > 2) args(2).toString else "normal"
    val partions = if (args.length > 3) args(3).toInt else 1

    val path = if (args.length > 4) args(4) else "data/swt/Classify" + distrib + time.getTime()

//        val path = "file:///E:/Shen/SparkFaultTolerant/DataSource/Classify"+ distrib + time.getTime()




//    var  i = 0
    val ranGen = new Random()
   distrib match {
/*       case "random" =>
        val pairs = {
          var pair = RandomRDDs.normalRDD(sc, ranGen.nextInt(instances)+1, partions).map(x => {
            i += 1
            (i, attributes + ":"+ x)
          })
          for (attr <- 1 until attributes) {
            var ins = 0
            pair = RandomRDDs.normalRDD(sc, ranGen.nextInt(instances)+1, partions).map(x => {
              ins += 1
              (ins, (attributes-attr) + ":" + x)
            }).union(pair)
          }
          pair.groupByKey(partions).values.map(x => (ranGen.nextInt(2), x))
        }.cache()
        pairs.saveAsTextFile(path)

      case "gamma" =>
        val pairs = {
          var pair = RandomRDDs.gammaRDD(sc, 9, 0.5, ranGen.nextInt(instances)+1, partions).map(x => {
            i += 1
            (i, attributes + ":"+ x)
          })
          for (attr <- 1 until attributes) {
            var ins = 0
            pair = RandomRDDs.gammaRDD(sc, 9, 0.5, ranGen.nextInt(instances)+1, partions).map(x => {
              ins += 1
              (ins, (attributes-attr) + ":" + x)
            }).union(pair)
          }
          pair.groupByKey(partions).values.map(x => (ranGen.nextInt(2), x))
        }.cache()
        pairs.saveAsTextFile(path)

      case "poisson" =>
        val pairs = {
          var pair = RandomRDDs.poissonRDD(sc, 1, ranGen.nextInt(instances)+1, partions).map(x => {
            i += 1
            (i, attributes+ ":" + x)
          })
          for (attr <- 1 until attributes) {
            var ins = 0
            pair = RandomRDDs.poissonRDD(sc, 1, ranGen.nextInt(instances)+1, partions).map(x => {
              ins += 1
              (ins, (attributes-attr) + ":" + x)
            }).union(pair)
          }
          pair.groupByKey(partions).values.map(x => (ranGen.nextInt(2), x))
        }.cache()
        pairs.saveAsTextFile(path)

      case "exponential" =>
        val pairs = {
          var pair = RandomRDDs.exponentialRDD(sc, 1, ranGen.nextInt(instances)+1, partions).map(x => {
            i += 1
            (i, attributes + ":"+ x)
          })
          for (attr <- 1 until attributes) {
            var ins = 0
            pair = RandomRDDs.exponentialRDD(sc, 1, ranGen.nextInt(instances)+1, partions).map(x => {
              ins += 1
              (ins, (attributes-attr) + ":" + x)
            }).union(pair)
          }
          pair.groupByKey(partions).values.map(x => (ranGen.nextInt(2), x))
        }.cache()
        pairs.saveAsTextFile(path)

      case "uniform" =>
        val pairs = {
          var pair = RandomRDDs.uniformRDD(sc, ranGen.nextInt(instances)+1, partions).map(x => {
            i += 1
            (i, attributes + ":"+ x)
          })
          for (attr <- 1 until attributes) {
            var ins = 0
            pair = RandomRDDs.uniformRDD(sc, ranGen.nextInt(instances)+1, partions).map(x => {
              ins += 1
              (ins, (attributes-attr) + ":" + x)
            }).union(pair)
          }
          pair.groupByKey(partions).values
        }.cache()
/*        val p = LabeledPoint(ranGen.nextInt(2), Vector(1))
        MLUtils.saveAsLibSVMFile(pairs, path)*/
        pairs.saveAsTextFile(path)*/


      case "uniform" =>
        val pairs = RandomRDDs.uniformVectorRDD(sc, instances, ranGen.nextInt(attributes), partions).map(x => LabeledPoint(ranGen.nextInt(2), x))
        MLUtils.saveAsLibSVMFile(pairs, path)

      case "normal" =>
        val pairs = RandomRDDs.normalVectorRDD(sc, instances, ranGen.nextInt(attributes), partions).map(x => LabeledPoint(ranGen.nextInt(2), x))
        MLUtils.saveAsLibSVMFile(pairs, path)

      case "gamma" =>
        val pairs = RandomRDDs.gammaVectorRDD(sc, 9, 0.5, instances, ranGen.nextInt(attributes), partions).map(x => LabeledPoint(ranGen.nextInt(2), x))
        MLUtils.saveAsLibSVMFile(pairs, path)

      case "poisson" =>
        val pairs = RandomRDDs.poissonVectorRDD(sc, 1, instances, ranGen.nextInt(attributes), partions).map(x => LabeledPoint(ranGen.nextInt(2), x))
        MLUtils.saveAsLibSVMFile(pairs, path)

      case "exponential" =>
        val pairs = RandomRDDs.exponentialVectorRDD(sc, 1, instances, ranGen.nextInt(attributes), partions).map(x => LabeledPoint(ranGen.nextInt(2), x))
        MLUtils.saveAsLibSVMFile(pairs, path)


    }
    sc.stop()
  }

}
