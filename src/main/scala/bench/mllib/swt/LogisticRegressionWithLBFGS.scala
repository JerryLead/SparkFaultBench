package bench.mllib.swt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by Shen on 2016/8/29.
  * usage: [data][numIterations][regParam][convergenceTol][numCorrections][numClasses]
  * example: $ spark-submit --class bench.mllib.swt.LogisticRegressionWithLBFGS  \
  *                         --master yarn     \
  *                         --deploy-mode cluster      \
  *                         --queue default  \
  *                         /usr/local/hadoop/shen/SparkFaultBench.jar \
  *                         data/mllib/sample_binary_classification_data.txt \
  *                         100 1.0 1E-6 10 2
  */
object LogisticRegressionWithLBFGS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LogisticRegressionWithLBFGSExample")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, args(0))

    val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val model = {
      val algorithm = new LogisticRegressionWithLBFGS()
      algorithm.optimizer
        .setNumIterations(args(1).toInt)  //100
        .setRegParam(args(2).toDouble)       //1.0
        .setConvergenceTol(args(3).toDouble)  //1E-6
        .setNumCorrections(args(4).toInt)     //10
      algorithm.setNumClasses(args(5).toInt).run(training)  //2
    }

    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")
    sc.stop()
  }
}
