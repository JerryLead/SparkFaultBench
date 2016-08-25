package bench.mllib.wjf

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by wjf on 2016/8/20.
  */
object MyNaiveBayesExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NaiveBayesExample")
      .setMaster("local[3]")
    val sc = new SparkContext(conf)
    // $example on$
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "hdfs://133.133.134.108:9000/user/hadoop/data/wjf/NaiveBayes_random_small.txt")


    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    // Save and load model
    println("accuracy"+accuracy)

    val myconf =new Configuration()
    myconf.set("fs.defaultFS","hdfs://133.133.134.108:9000")
    val fs =FileSystem.get(myconf)
    val output =fs.create(new Path(args(args.length-1)))
    val writer =new PrintWriter(output)
    writer.write("accuracy: "+accuracy.toString+"\n")
    writer.close()
    println("write success")

//    predictionAndLabel.foreach(println)
    model.save(sc, "hdfs://133.133.134.108:9000/user/hadoop/data/wjf/myNaiveBayesModel")
    val sameModel = NaiveBayesModel.load(sc, "hdfs://133.133.134.108:9000/user/hadoop/data/wjf/myNaiveBayesModel")
    // $example off$
  }
}
