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
package mllib.wjf

import org.apache.spark.{SparkConf, SparkContext}
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


    val data = sc.textFile("hdfs://133.133.134.108:9000/user/hadoop/data/wjf/Kmeans_random.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20


    val clusters = KMeans.train(parsedData, numClusters, numIterations,10)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
      clusters.save(sc, "hdfs://133.133.134.108:9000/user/hadoop/data/wjf/KMeansExample/KMeansModelBig")
    val sameModel = KMeansModel.load(sc, "hdfs://133.133.134.108:9000/user/hadoop/data/wjf/KMeansExample/KMeansModelBig")
    // $example off$
    sameModel.clusterCenters.foreach(println)
    sc.stop()
  }
}
// scalastyle:on println
