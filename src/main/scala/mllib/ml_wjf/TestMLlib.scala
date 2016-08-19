package mllib.ml_wjf

//import com.sun.xml.internal.ws.util.ByteArrayBuffer
//import org.apache.hadoop.hive.ql.exec.vector.VectorAssignRowSameBatch
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector,Vectors}
//import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row

/**
  * Created by wjf on 2016/8/11.
  */
object TestMLlib {
//  System.setProperty("hadoop.home.dir", "D://development//hadoop-2.6.4")
  var spark=SparkSession.builder().master("local").appName("spark_mllib")
    //  seem to set workspace
      .config("spark.sql.warehouse.dir","file:///d://data")
    .getOrCreate()
  //  var conf=new SparkConf().setAppName("spark_mllib")
  //  conf.setMaster("local")
  //  var sc=new SparkContext(conf)
  def main(args: Array[String]): Unit = {
    var home=System.getProperty("hadoop.home.dir")
    println(home)

    home=System.getProperty("hadoop.home.dir")
    println(home)
    val training=spark.createDataFrame(Seq(
      (1.0,Vectors.dense(0.0,1.1,0.1)),
      (0.0,Vectors.dense(2.0,1.0,-1.0)),
      (0.0,Vectors.dense(2.0,1.3,1.0)),
      (1.0,Vectors.dense(0.0,1.2,-0.5))
    )).toDF("label","features")
    val lr=new LogisticRegression()
    println("LogisticRegression parameters:\n"+lr.explainParams()+"\n")
    lr.setMaxIter(10).setRegParam(0.01)

    val model1=lr.fit(training)
    println("Model 1 was fit using parameters:"+model1.parent.extractParamMap())
    val paramMap =ParamMap(lr.maxIter -> 20).put(lr.maxIter,30)
      .put(lr.regParam -> 0.1,lr.threshold -> 0.55)

    val paramMap2=ParamMap(lr.probabilityCol -> "myProbability")
    val paramMapCombined = paramMap ++ paramMap2
    val model2= lr.fit(training,paramMapCombined)

    println("model 2 was fit using parameters:"+ model2.parent.extractParamMap())

    val test=spark.createDataFrame(Seq(
      (1.0,Vectors.dense(-1.0,1.5,1.3)),
      (0.0,Vectors.dense(3.0,2.0,-0.1)),
      (1.0,Vectors.dense(0.0,2.2,-1.5)
        )
    )).toDF("label","features")
    model2.transform(test)
      .select("features","label","myProbability","prediction")
      .collect().foreach{
      case Row(features: Vector,label:Double,prob:Vector,prediction:Double)=>
        println(s"($features,$label 0> prob =$prob,prediction = $prediction")
    }
  }


}

