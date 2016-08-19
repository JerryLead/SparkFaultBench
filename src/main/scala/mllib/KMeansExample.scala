/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package mllib

import breeze.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random
// $example on$
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
// $example off$

object KMeansExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("KMeansExample")
//    conf.setMaster("local")
//    conf.set("spark.sql.warehouse.dir","file:///")
//    conf.set("spark.sql.warehouse.dir","file:///")
    val sc = new SparkContext(conf)

    /*
    * generate random input data by wjf*/
    var num_of_points=10000000;
    var dim_of_points=2;
    val input_array= new Array[Seq[Int]](num_of_points)
    var random =new Random()
    for(i<-0 until 10)println(random.nextGaussian())

    for(i <- 0 until num_of_points){
      input_array(i) = Seq(random.nextInt(100),random.nextInt(100))

    }
      input_array.sa
    var parsedData= sc.parallelize(input_array.map( s => Vectors.dense(s(0), s(1))))
    // $example on$
    // Load and parse the data
    // this is default input
//    val data = sc.textFile("D://development//intellij_idea//workspace//SparkFaultBench//data//mllib//kmeans_data.txt")
//    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
//    parsedData.foreach(println)

    val clusters = KMeans.train(parsedData, numClusters, numIterations,10)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    // $example off$
    sameModel.clusterCenters.foreach(println)
    sc.stop()
  }
}
// scalastyle:on println
