package mllib.wjf

import java.io.{File, PrintWriter}

import mllib.HdfsHelp

import scala.util.Random

/**
  * Created by wjf on 2016/8/19.
  */
object GenerateKMeansData {

  def main(args: Array[String]): Unit = {
    val path ="dataGenerated/mllib/wjf/Kmeans_random_small.txt"
    randomData(path)
    HdfsHelp.uploadFile2HDFS("dataGenerated/mllib/wjf/Kmeans_random_small.txt", "hdfs://133.133.134.108:9000/user/hadoop/data/wjf")

  }
  def randomData(path:String): Unit ={
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

}
