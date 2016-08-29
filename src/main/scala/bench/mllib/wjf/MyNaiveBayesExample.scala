package bench.mllib.wjf

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by wjf on 2016/8/20.
  */
object MyNaiveBayesExample {
  val conf = new SparkConf().setAppName("NaiveBayesExample")

  val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
//     test_special_data
       test_random()

  }
  def test_random(): Unit ={
    // $example on$
    // Load and parse the data file.
    //    val data = MLUtils.loadLibSVMFile(sc, "hdfs://133.133.134.108:9000/user/hadoop/data/wjf/NaiveBayes_random_small.txt")
    val path ="hdfs://133.133.134.108:9000/user/hadoop/data/wjf/NaiveBayes_random_small.txt"
    val rawData= sc.textFile(path)
    val data =rawData.map{ line =>

      val parts= line.split(",")
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }
//    val data =MLUtils.loadLibSVMFile(sc,path)

    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4),seed = 11L)
    val model =NaiveBayes.train(training,lambda = 1.0,modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    // Save and load model
    println("accuracy"+accuracy)
    predictionAndLabel.take(20).foreach(println)

    val myconf =new Configuration()
    myconf.set("fs.defaultFS","hdfs://133.133.134.108:9000")
    val fs =FileSystem.get(myconf)
    //    val output =fs.create(new Path(args(args.length-1)))
    //    val writer =new PrintWriter(output)
    //    writer.write("accuracy: "+accuracy.toString+"\n")
    //    writer.close()
    println("write success")

    //    predictionAndLabel.foreach(println)
//    model.save(sc, "hdfs://133.133.134.108:9000/user/hadoop/data/wjf/myNaiveBayesModelBig")
//    val sameModel = NaiveBayesModel.load(sc, "hdfs://133.133.134.108:9000/user/hadoop/data/wjf/myNaiveBayesModel")
    // $example off$
  }

  def test_special_data(): Unit ={
    val raw_train = sc.parallelize(Seq(
      Seq(4.0,1.0,1.0,1.0),
      Seq(6.0,1.0,1.0,1.0)))
    val training =raw_train.map{ seq =>
      LabeledPoint(seq(0),Vectors.dense(seq.drop(1).toArray))
    }
    val raw_test=sc.parallelize(Seq(Seq(4.0,0.0,0.0,0.0)))
    val test = raw_test.map{ seq =>
      LabeledPoint(seq(0),Vectors.dense(seq.drop(1).toArray))
    }

    val model =NaiveBayes.train(training,lambda = 1.0,modelType="multinomial")
//    val model =NaiveBayes.train(training,lambda = 1.0,modelType = "multinomial")
    val predictionAndLabel =test.map(p => (model.predict(p.features),p.label))
    val accuracy =1.0 * predictionAndLabel.filter(x => x._1 == x._2).count()/ test.count()

    predictionAndLabel.take(10).map(p => println(p._1,p._2,"success"))
    println("accuracy "+ accuracy)

  }
}
