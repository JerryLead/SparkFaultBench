package sql.standard

import org.apache.spark.sql.SparkSession
import BaseUtils._
/**
  * Created by lenovo on 2016/8/24 0024.
  */
object Mix {
  val rankingsName = "rankings"
  val uservisitsName = "uservisits"
  def main(args: Array[String]): Unit = {
    var dfs_path = "dataGenerated/sql/lcr/scripts/"
    var scale = "1"
    var testType = "both"
    var degree = ""
    if (args.length>0) dfs_path = getHDFSPath(args(0))
    if (args.length>1) scale = args(1)
    if (args.length>2) testType = args(2)
    if (args.length>3) degree = args(3)
    if (testType != "skewed"){
      val file1 = genFileFullName(rankingsName,scale,"normal")
      val file2 = genFileFullName(uservisitsName,scale,"normal")
      val spark = getSparkSession("Join")
      doMixSQL(spark,dfs_path,file1,file2)
    }
    if (testType != "normal"){
      val file1 = genFileFullName(rankingsName,scale,"skewed")
      val file2 = genFileFullName(uservisitsName,scale,"skewed")
      val spark = getSparkSession("SkewJoin")
      doMixSQL(spark,dfs_path,file1,file2)
    }

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
              "FROM Uservisits AS UV, Rankings AS R " +
              "WHERE R.url = UV.destinationURL " +
                  "AND UV.avgTimeOnSite BETWEEN 8 AND 95 " +
              "GROUP BY UV.destinationURL) " +
              "ORDER BY totalRevenue DESC LIMIT 10"
    val mixDF = spark.sql(sqltext)
    mixDF.show()
  }
}
