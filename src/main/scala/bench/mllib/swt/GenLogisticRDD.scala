package bench.mllib.swt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.{LogisticRegressionDataGenerator, MLUtils}

/**
  * Created by Shen on 2017/1/10.
  * parameters: [n_examples][n_features][eps][n_parts][probOne][path]
  */
object GenLogisticRDD {
  def main(args: Array[String]){
    val conf = new SparkConf()
//            .setAppName("GenerateLogisticRDD")
//          .set("spark.sql.warehouse.dir", "file:///E:/Shen/spark-warehouse")
          .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = LogisticRegressionDataGenerator.generateLogisticRDD(sc,
      args(0).toInt, args(1).toInt, args(2).toDouble, args(3).toInt, args(4).toDouble)
    MLUtils.saveAsLibSVMFile(data, args(5))

    sc.stop()
  }

}
