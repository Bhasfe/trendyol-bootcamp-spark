package com.trendyol.bootcamp.homework

import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import scala.util.Try

object ProductMergerJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Trendyol Week5 Homework")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(
      List(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("category", StringType),
        StructField("brand", StringType),
        StructField("color", StringType),
        StructField("price", FloatType),
        StructField("timestamp", LongType)
      )
    )

    val upToDateData = Try(
      spark.read
        .schema(schema)
        .json("data/homework/uptodate_data.json")
        .withColumn("timestamp", from_unixtime($"timestamp" / 1000))
    ).getOrElse(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema))

    val path = {
      if (upToDateData.count()==0){
        "data/homework/initial_data.json"
      } else {
        "data/homework/cdc_data.json"
      }
    }
    val cdcData = spark.read
      .schema(schema)
      .json(path)
      .withColumn("timestamp", from_unixtime($"timestamp" / 1000))

    // upToDateData.show()
    // cdcData.show()

    val rankWindow = Window.partitionBy("id").orderBy(col("timestamp").desc)

    val target = upToDateData.union(cdcData)
      .withColumn("rownum", row_number().over(rankWindow))
      .filter(col("rownum") === 1)
      .drop("rownum")
      .orderBy("id")
      .repartition(1)
      .write
      .json("data/homework/uptodate_data.json")



    // target.show()
  }
}