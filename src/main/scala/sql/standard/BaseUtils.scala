package sql.standard

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by lenovo on 2016/8/24 0024.
  */
object BaseUtils {
  val HDFS_PATH = "hdfs:///user/hadoop/data/lcr/"
  case class Rankings(pagerank: Long, url: String, adprofit:Long)
  case class UserVisits(sourceIPAddr:String,
                        destinationURL: String,
                        visitDate:Long,
                        adRevenue:Double,
                        UserAgent:String,
                        cCode:String,
                        lCode:String,
                        sKeyword:String,
                        avgTimeOnSite:Long
                       )

  def genFileFullName(name: String,scale:String,testType:String):String={
      val file1 = name+"_"+testType+"_"+scale+"G"
      return file1
  }

  def getHDFSPath(s:String): String ={
    var path = ""
    path = "hdfs://"+s
    if (path.charAt(path.length-1) != '/'){
      path = path+'/'
    }
    return path
  }

  def getSparkSession(appName:String):SparkSession={
    val warehouseLocation = System.getProperty("user.dir")
    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.warehouse.dir",warehouseLocation)
      //.master("local[5]")
      .getOrCreate()
    return spark
  }

  def getRankingsDF(spark:SparkSession,loadfile:String, path:String):DataFrame={
    import spark.implicits._
    val rankingsDF = spark.sparkContext
      .textFile(path + loadfile)
      .map(_.split(","))
      .map(attributes=>Rankings(attributes(0).replace("(","").trim.toInt,
                                attributes(1),
                                attributes(2).replace(")","").trim.toInt))
      .toDF()
    return rankingsDF
    }

  def getUservisitsDF(spark:SparkSession,loadfile:String, path:String):DataFrame={
    import spark.implicits._
    val uservisitsDF =spark.sparkContext
      .textFile(path + loadfile)
      .map(_.split(","))
      .map(attributes=>UserVisits(attributes(0).replace("(",""),
        attributes(1),
        attributes(2).trim.toLong,
        attributes(3).trim.toDouble,
        attributes(4),
        attributes(5),
        attributes(6),
        attributes(7),
        attributes(8).replace(")","").trim.toLong)).toDF()
    return uservisitsDF
  }
}
