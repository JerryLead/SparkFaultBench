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
package sql

// $example on:schema_inferring$
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
// $example off:schema_inferring$
import org.apache.spark.sql.Row
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$

object SparkSQLExample {

  // $example on:create_ds$
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface
  case class Rankings(pagerank: Long, url: String, adprofit:Long)
  // $example off:create_ds$

  def main(args: Array[String]) {
    // $example on:init_session$
    val abc = System.getProperty("user.dir")
    val spark = SparkSession
      .builder()
      .appName("Scan SQL")
      .config("spark.some.config.option", "some-value")
        .config("spark.sql.warehouse.dir",abc)

          .master("local[2]")
      .getOrCreate()

    runInferSchemaExample(spark)

    spark.stop()
  }

  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleDF = spark.sparkContext
      .textFile("dataGenerated/sql/lcr/sqldata/rankings.txt")
      .map(_.split(","))
      .map(attributes => Rankings(attributes(0).trim.toInt, attributes(1).trim,attributes(2).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("rankings")

    val teenagersDF = spark.sql("SELECT url, pagerank FROM rankings")

    teenagersDF.map(teenager => "url: " + teenager(0)).show()

    teenagersDF.map(teenager => "url: " + teenager.getAs[String]("url")).show()

   // implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

   // implicit val stringIntMapEncoder: Encoder[Map[String, Int]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
   // teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))
    // $example off:schema_inferring$
  }

}
