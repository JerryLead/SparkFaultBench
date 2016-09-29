package sql.standard

import BaseUtils._
import org.apache.spark.sql.SparkSession
/**
  * Created by lenovo on 2016/8/23 0023.
  */

object Scan {

  val rankingsName = "rankings"
  val uservisitsName = "uservisits"
  def main(args: Array[String]): Unit = {
    var dfs_path = "dataGenerated/sql/lcr/scripts/"
    var scale = "1"
    var testType = "both"
    var degree = "2.0"
    if (args.length>0) dfs_path = getHDFSPath(args(0))
    if (args.length>1) scale = args(1)
    if (args.length>2) testType = args(2)
    if (args.length>3) degree = args(3)
    if (testType != "skewed"){
      val file1 = genFileFullName(rankingsName,scale,"normal")
      val file2 = genFileFullName(uservisitsName,scale,"normal")
      val spark = getSparkSession("Join")
      doScanSQL(spark,dfs_path,file1,file2)
    }
    if (testType != "normal"){
      val file1 = genFileFullName(rankingsName,scale,"skewed")
      val file2 = genFileFullName(uservisitsName,scale,"skewed")
      val spark = getSparkSession("SkewJoin")
      doScanSQL(spark,dfs_path,file1,file2)
    }

  }
  private def doScanSQL(spark:SparkSession, dfs_path:String, file1:String, file2:String):Unit={
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
