package basic

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xulijie on 16-6-30.
  */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)


    var filePath = "src/main/scala/basic/WordCount.scala"
    filePath="d://data//123.txt"
    val textFile = sc.textFile(filePath)
    val result = textFile.flatMap(_.split("[ |\\.]"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    result.collect().foreach(println)
    sc.stop()
  }
}
