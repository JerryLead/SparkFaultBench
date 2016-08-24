package sql

import org.apache.spark.sql.SparkSession
import sql.BaseUtils._

/**
  * Created by lenovo on 2016/8/24 0024.
  */
object Join {


  def main(args: Array[String]): Unit = {
    val warehouseLocation = System.getProperty("user.dir")
    val spark = SparkSession.builder()
      .appName("Join")
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .master("local[2]")
      .getOrCreate()

    doJoinSQL(spark)
  }

  def doJoinSQL(spark:SparkSession) : Unit={
    import spark.implicits._
    val rankingsDF = getRankingsDF(spark)
    val uservisitsDF = getUservisitsDF(spark)

    rankingsDF.createOrReplaceTempView("rankings")
    uservisitsDF.createOrReplaceTempView("uservisits")
    val sqltext = ""
    val joinDF = spark.sql("select sourceipaddr,url,adrevenue from rankings INNER JOIN uservisits ON url=destinationURL")
    joinDF.show()
    //print(joinDF.count())

  }

}

