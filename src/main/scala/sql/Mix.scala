package sql

import org.apache.spark.sql.SparkSession
import sql.BaseUtils._

/**
  * Created by lenovo on 2016/8/24 0024.
  */
object Mix {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession("Mix")
    doMixSQL(spark)
  }
  def doMixSQL(spark: SparkSession) :Unit={
    val rankingsDF = getRankingsDF(spark)
    val uservisitsDF = getUservisitsDF(spark)

    rankingsDF.createOrReplaceTempView("rankings")
    uservisitsDF.createOrReplaceTempView("uservisits")

    val sqltext =
      "SELECT sourceIPAddr, totalRevenue, avgPageRank " +
      "FROM (SELECT sourceIPAddr," +
                      "AVG(pageRank) as avgPageRank," +
                      "SUM(adRevenue) as totalRevenue " +
              "FROM Rankings AS R, Uservisits AS UV " +
              "WHERE R.url = UV.destinationURL " +
                  "AND UV.avgTimeOnSite BETWEEN 30 AND 70 " +
              "GROUP BY UV.sourceIPAddr) " +
              "ORDER BY totalRevenue DESC LIMIT 10"
    val mixDF = spark.sql(sqltext)
    mixDF.show()
  }
}
