package sql.standard

import org.apache.spark.sql.SparkSession
import BaseUtils._
/**
  * Created by lenovo on 2016/8/24 0024.
  */
object Aggregate {
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
      val spark = getSparkSession("Aggregate")
      doAggregateSQL(spark,dfs_path,file1,file2)
    }
    if (testType != "normal"){
      val file1 = genFileFullName(rankingsName,scale,"skewed")
      val file2 = genFileFullName(uservisitsName,scale,"skewed")
      val spark = getSparkSession("SkewAggregate")
      doAggregateSQL(spark,dfs_path,file1,file2)
    }

  }

  def doAggregateSQL(spark:SparkSession, dfs_path:String, file1:String, file2:String): Unit={
//    val rankingsDF = getRankingsDF(spark)
    val uservisitsDF = getUservisitsDF(spark,file2,dfs_path)

//    rankingsDF.createOrReplaceTempView("rankings")
    uservisitsDF.createOrReplaceTempView("uservisits")
    val sqltext = "select destinationURL,sum(adRevenue) as total from uservisits group by destinationURL order by total desc"
    val aggreDF = spark.sql(sqltext)
    aggreDF.show()
    val sqltext2 = "select count(distinct destinationURL) AS url_count from uservisits"
    val aggreDF2 = spark.sql(sqltext2)
    aggreDF2.show()
  }
}

