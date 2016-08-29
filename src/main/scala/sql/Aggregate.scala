package sql

import org.apache.spark.sql.SparkSession
import sql.BaseUtils._

/**
  * Created by lenovo on 2016/8/24 0024.
  */
object Aggregate {
  def main(args: Array[String]): Unit = {
    val dfs_path = args(0)
    val file1 = args(1)
    val file2 = args(2)
    val spark = getSparkSession("Aggregate")
    doAggregateSQL(spark,dfs_path,file1,file2)
  }

  def doAggregateSQL(spark:SparkSession, dfs_path:String, file1:String, file2:String): Unit={
    import spark.implicits._
//    val rankingsDF = getRankingsDF(spark)
    val uservisitsDF = getUservisitsDF(spark,file2,dfs_path)

//    rankingsDF.createOrReplaceTempView("rankings")
    uservisitsDF.createOrReplaceTempView("uservisits")
    val sqltext = "select destinationURL,sum(adRevenue) as total from uservisits group by destinationURL order by total desc"
    val aggreDF = spark.sql(sqltext)
    aggreDF.show()

  }
}

