package bench.mllib.wjf

import java.io.{File, PrintWriter}



import scala.util.Random

/**
  * Created by wjf on 2016/8/19.
  */
object RandomDataGenerator {

  def main(args: Array[String]): Unit = {
//    val path ="dataGenerated/mllib/wjf/Kmeans_random_small.txt"
//    kmeansRandomData(path)
//    HdfsHelp.uploadFile2HDFS("dataGenerated/mllib/wjf/Kmeans_random_small.txt", "hdfs://133.133.134.108:9000/user/hadoop/data/wjf")

    // naiveBayesRandomData
    val path= "dataGenerated/mllib/wjf/NaiveBayes_random_small.txt"
    naiveBayesRandomData(path)
    HdfsHelp.uploadFile2HDFS("dataGenerated/mllib/wjf/NaiveBayes_random_small.txt", "hdfs://133.133.134.108:9000/user/hadoop/data/wjf")

  }
  def kmeansRandomData(path:String): Unit ={
    // Size: 580 MB
    var num_of_points=10000000;
    var dim_of_points=2;
    val input_array= new Array[Seq[Int]](num_of_points)
    var random =new Random()
//    for(i<-0 until 10)println(random.nextGaussian())

    val writer =new PrintWriter(new File(path))
    for(i <- 0 until num_of_points){
//      input_array(i) = Seq(random.nextInt(100),random.nextInt(100))
      writer.write( random.nextInt(100).toString+" "+random.nextInt(100).toString+"\n")
    }
    writer.close()
  }
  def naiveBayesRandomData(path:String):Unit ={
    // data format is LabledPoints
    // big 1000000  500  small 1000 50
    val num_of_points=1000000
    val num_of_dim=500

    val random =new Random()
    var temp =1;
    val writer =new PrintWriter(new File(path))
    for(i <- 0 until num_of_points){
      writer.write(random.nextInt(10).toString+",")
      for(j <- 0 until num_of_dim){
        writer.write(random.nextInt(10).toString + " ")
      }
      writer.write("\n")
    }
    writer.close()





  }


}
