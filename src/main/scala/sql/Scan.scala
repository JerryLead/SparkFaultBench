package sql

import org.apache.spark.sql.SparkSession
import sql.BaseUtils._
/**
  * Created by lenovo on 2016/8/23 0023.
  */

object Scan {


  def main(args: Array[String]): Unit = {

    val warehouseLocation = System.getProperty("user.dir")
    val spark = getSparkSession("Scan")
    doScanSQL(spark)
  }
  private def doScanSQL(spark:SparkSession):Unit={
    import spark.implicits._
    val rankingsDF = getRankingsDF(spark,"rankings.txt")

    rankingsDF.createOrReplaceTempView("rankings")
    val scanDF = spark.sql("SELECT * From rankings where pagerank > 20")
    scanDF.show()

    val uservisitsDF =getUservisitsDF(spark,"uservisits.txt")

    uservisitsDF.createOrReplaceTempView("uservisits")
    val scanDF2 = spark.sql("SELECT * from uservisits")
    scanDF2.show()
  }

}
