package sql.standard

import org.apache.spark.sql.SparkSession
import BaseUtils._
/**
  * Created by lenovo on 2016/8/24 0024.
  */
object Join {

  def main(args: Array[String]): Unit = {
    val dfs_path = args(0)
    val file1 = args(1)
    val file2 = args(2)
    val spark = getSparkSession("Join")

    doJoinSQL(spark,dfs_path,file1,file2)
  }

  def doJoinSQL(spark:SparkSession, dfs_path:String, file1:String, file2:String) : Unit={
    val rankingsDF = getRankingsDF(spark,file1,dfs_path)
    val uservisitsDF = getUservisitsDF(spark,file2,dfs_path)

    rankingsDF.createOrReplaceTempView("rankings")
    uservisitsDF.createOrReplaceTempView("uservisits")
    val sqltext = "select pagerank,sourceipaddr,url,adrevenue " +
      "from uservisits " +
      "INNER JOIN rankings " +
      "ON url=destinationURL " +
      "order by adrevenue desc limit 100"
    val joinDF = spark.sql(sqltext)
    joinDF.show()
    //print(joinDF.count())

  }

}

