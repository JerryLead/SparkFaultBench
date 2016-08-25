package sql

import org.apache.spark.sql.SparkSession
import sql.BaseUtils._

/**
  * Created by lenovo on 2016/8/24 0024.
  */
object Aggregate {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = System.getProperty("user.dir")
    val spark = getSparkSession("Aggregate")
    doAggregateSQL(spark)
  }

  def doAggregateSQL(spark:SparkSession): Unit={
    import spark.implicits._
//    val rankingsDF = getRankingsDF(spark)
    val uservisitsDF = getUservisitsDF(spark)

//    rankingsDF.createOrReplaceTempView("rankings")
    uservisitsDF.createOrReplaceTempView("uservisits")
    val sqltext = "select substr(sourceipaddr,1,3),sum(adRevenue) from uservisits group by substr(sourceipaddr,1,3)"
    val aggreDF = spark.sql(sqltext)
    aggreDF.show()

  }
}

