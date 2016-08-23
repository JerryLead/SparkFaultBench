package sql

import org.apache.spark.sql.SparkSession
/**
  * Created by lenovo on 2016/8/23 0023.
  */

object Scan {
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

  def main(args: Array[String]): Unit = {

    val warehouseLocation = System.getProperty("user.dir")+"spark-warehouse"
    val spark = SparkSession.builder()
      .appName("Scan")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .master("local[2]").getOrCreate()
    doScanSQL(spark)
  }
  private def doScanSQL(spark:SparkSession):Unit={
    import spark.implicits._
    val rankingsDF = spark.sparkContext
      .textFile("dataGenerated/sql/lcr/sqldata/rankings.txt")
      .map(_.split(","))
      .map(attributes=>Rankings(attributes(0).trim.toInt,attributes(1),attributes(2).trim.toInt))
      .toDF()

    rankingsDF.createOrReplaceTempView("rankings")
    val scanDF = spark.sql("SELECT * From rankings")
    scanDF.show()

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
    uservisitsDF.createOrReplaceTempView("uservisits")
    val scanDF2 = spark.sql("SELECT * from uservisits")
    scanDF2.show()
  }

}
