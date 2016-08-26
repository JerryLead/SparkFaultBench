package sql

import org.apache.spark.sql.SparkSession
import sql.BaseUtils._

/**
  * Created by lenovo on 2016/8/24 0024.
  */
object Join {


  def main(args: Array[String]): Unit = {
    val warehouseLocation = System.getProperty("user.dir")
    val spark = getSparkSession("Join")

    doJoinSQL(spark)
  }

  def doJoinSQL(spark:SparkSession) : Unit={
    import spark.implicits._
    val rankingsDF = getRankingsDF(spark,"rankings.txt")
    val uservisitsDF = getUservisitsDF(spark,"uservisits.txt")

    rankingsDF.createOrReplaceTempView("rankings")
    uservisitsDF.createOrReplaceTempView("uservisits")
    val sqltext = "select sourceipaddr,url,adrevenue from rankings INNER JOIN uservisits ON url=destinationURL"
    val joinDF = spark.sql(sqltext)
    joinDF.show()
    //print(joinDF.count())

  }

}

