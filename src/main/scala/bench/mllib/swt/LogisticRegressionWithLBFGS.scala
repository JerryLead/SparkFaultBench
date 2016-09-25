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

    val path = args(0)
    val numIterations = if (args.length > 1) args(1).toInt else 10
    val regParam = if (args.length > 2) args(2).toDouble else 1.0
    val convergenceTol = if (args.length > 3) args(3).toDouble else 1E-6
    val numCorrections = if (args.length > 4) args(4).toInt else 10
    val numClasses = if (args.length > 4) args(4).toInt else 2
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, path)

    val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val model = {
      val algorithm = new LogisticRegressionWithLBFGS()
      algorithm.optimizer
        .setNumIterations(numIterations)  //100
        .setRegParam(regParam)       //1.0
        .setConvergenceTol(convergenceTol)  //1E-6
        .setNumCorrections(numCorrections)     //10
      algorithm.setNumClasses(numClasses).run(training).setThreshold(0.5)  //2

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
