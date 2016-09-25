package bench.mllib.swt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by Shen on 2016/8/29.
  * usage: [data][numClasses][numTrees][maxDepth][maxBins]
  * example: $ spark-submit --class bench.mllib.swt.RandomForestClassification  \
  *                         --master yarn     \
  *                         --deploy-mode cluster      \
  *                         --queue default  \
  *                         /usr/local/hadoop/shen/SparkFaultBench.jar \
  *                         data/mllib/sample_binary_classification_data.txt \
  *                         2 2 5 3
  */

object RandomForestClassification {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RandomForestClassification")
    val sc = new SparkContext(conf)

    val path = args(0)
    val numClasses = if (args.length > 1) args(1).toInt else 2
    val numTrees = if (args.length > 2) args(2).toInt else 2
    val maxDepth = if (args.length > 3) args(3).toInt else 5
    val maxBins = if (args.length > 4) args(4).toInt else 32

    val data = MLUtils.loadLibSVMFile(sc, path)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val categoricalFeaturesInfo = Map[Int, Int]()
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)

  }
}

