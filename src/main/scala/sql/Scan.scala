package sql

import org.apache.spark.sql.SparkSession
import sql.BaseUtils._
/**
  * Created by lenovo on 2016/8/23 0023.
  */

object Scan {


  def main(args: Array[String]): Unit = {

    val dfs_path = args(0)
    val file1 = args(1)
    val file2 = args(2)
    val spark = getSparkSession("Scan")
    doScanSQL(spark,dfs_path,file1,file2)
  }
  private def doScanSQL(spark:SparkSession, dfs_path:String, file1:String, file2:String):Unit={
    import spark.implicits._
    val rankingsDF = getRankingsDF(spark,file1,dfs_path)

    rankingsDF.createOrReplaceTempView("rankings")
    val scanDF = spark.sql("SELECT * From rankings where pagerank > 20")
    scanDF.show()

    val uservisitsDF =getUservisitsDF(spark,file2,dfs_path)

    uservisitsDF.createOrReplaceTempView("uservisits")
    val scanDF2 = spark.sql("SELECT * from uservisits where adRevenue > 200.0")
    scanDF2.show()
  }

}
