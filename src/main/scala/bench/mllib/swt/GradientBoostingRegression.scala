package bench.mllib.swt

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

/**
  * Created by Shen on 2017/1/10.
  * parameters: [train_data][test_data][n_iterations][max_depth]
  */

object GradientBoostingRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GradientBoostedTreesRegression")
//      .set("spark.sql.warehouse.dir", "file:///E:/Shen/spark-warehouse")
//      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val trainingData = MLUtils.loadLibSVMFile(sc, args(0))
    val testData = MLUtils.loadLibSVMFile(sc, args(1))

    // Train a GradientBoostedTrees model.
    // The defaultParams for Regression use SquaredError by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = args(2).toInt // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.maxDepth = args(3).toInt
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression GBT model:\n" + model.toDebugString.substring(0,1000))

    model.save(sc, "target/tmp/myGradientBoostingRegressionModel")
    val time = new java.util.Date
    val sameModel = GradientBoostedTreesModel.load(sc,
      "target/tmp/myGBDTModel" + time.getTime())

    sc.stop()
  }
}
