package sql.standard

import org.apache.spark.sql.SparkSession
import BaseUtils._
/**
  * Created by lenovo on 2016/8/24 0024.
  */
object Join {
  val rankingsName = "rankings"
  val uservisitsName = "uservisits"
  def main(args: Array[String]): Unit = {
    var dfs_path = "dataGenerated/sql/lcr/scripts/"
    var scale = "1"
    var testType = "both"
    if (args.length>0) dfs_path = getHDFSPath(args(0))
    if (args.length>1) scale = args(1)
    if (args.length>2) testType = args(2)

    if (testType != "skewed"){
      val file1 = genFileFullName(rankingsName,scale,"normal")
      val file2 = genFileFullName(uservisitsName,scale,"normal")
      val spark = getSparkSession("Join")
      doJoinSQL(spark,dfs_path,file1,file2)
    }
    if (testType != "normal"){
      val file1 = genFileFullName(rankingsName,scale,"skewed")
      val file2 = genFileFullName(uservisitsName,scale,"skewed")
      val spark = getSparkSession("SkewJoin")
      doJoinSQL(spark,dfs_path,file1,file2)
    }

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

