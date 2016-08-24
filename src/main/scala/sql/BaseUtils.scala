package sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by lenovo on 2016/8/24 0024.
  */
object BaseUtils {
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
  def getRankingsDF(spark:SparkSession):DataFrame={
    import spark.implicits._
    val rankingsDF = spark.sparkContext
      .textFile("dataGenerated/sql/lcr/sqldata/rankings.txt")
      .map(_.split(","))
      .map(attributes=>Rankings(attributes(0).trim.toInt,attributes(1),attributes(2).trim.toInt))
      .toDF()
    return rankingsDF
  }

  def getUservisitsDF(spark:SparkSession):DataFrame={
    import spark.implicits._
    val uservisitsDF =spark.sparkContext
      .textFile("dataGenerated/sql/lcr/sqldata/uservisits.txt")
      .map(_.split(","))
      .map(attributes=>UserVisits(attributes(0),
        attributes(1),
        attributes(2).trim.toLong,
        attributes(3).trim.toDouble,
        attributes(4),
        attributes(5),
        attributes(6),
        attributes(7),
        attributes(8).trim.toLong)).toDF()
    return uservisitsDF
  }
}
