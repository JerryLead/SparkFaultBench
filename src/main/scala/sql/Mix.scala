package sql

import org.apache.spark.sql.SparkSession
import sql.BaseUtils._

/**
  * Created by lenovo on 2016/8/24 0024.
  */
object Mix {
  def main(args: Array[String]): Unit = {
    val dfs_path = args(0)
    val file1 = args(1)
    val file2 = args(2)
    val spark = getSparkSession("Mix")
    doMixSQL(spark,dfs_path,file1,file2)
  }
  def doMixSQL(spark:SparkSession, dfs_path:String, file1:String, file2:String) :Unit={
    val rankingsDF = getRankingsDF(spark,file1,dfs_path)
    val uservisitsDF = getUservisitsDF(spark,file2,dfs_path)

    rankingsDF.createOrReplaceTempView("rankings")
    uservisitsDF.createOrReplaceTempView("uservisits")

    val sqltext =
      "SELECT destinationURL,totalRevenue, avgPageRank " +
      "FROM (SELECT destinationURL," +
                      "AVG(pageRank) as avgPageRank," +
                      "SUM(adRevenue) as totalRevenue " +
              "FROM Rankings AS R, Uservisits AS UV " +
              "WHERE R.url = UV.destinationURL " +
                  "AND UV.avgTimeOnSite BETWEEN 8 AND 95 " +
              "GROUP BY UV.destinationURL) " +
              "ORDER BY totalRevenue DESC LIMIT 10"
    val mixDF = spark.sql(sqltext)
    mixDF.show()
  }
}
