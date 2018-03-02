package com.pari.poc.grab

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * @author Parivallal R
  *
  */

object SurgePriceCalculator {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val batchIntervalSeconds = 5 //  seconds
    val windowInterval = Seconds(10)
    val slidingInterval = Seconds(5)

    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("GRAB")

    val ssc = new StreamingContext(conf, Seconds(batchIntervalSeconds))

    val spark = SparkSession.builder().appName("GRAB")
      .config(conf)
      .getOrCreate()


    /*
        UDF to get level 6 GeoHash(600M X 1200M) of given lat lng
         */
    spark.udf.register("myUDF", (latlng: String) => {
      val precision = 6
      val temp = latlng.split("|")
      val lat = temp(0).toDouble
      val lng = temp(1).toDouble
      val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
      var (minLat, maxLat) = (-90.0, 90.0)
      var (minLng, maxLng) = (-180.0, 180.0)
      val bits = List(16, 8, 4, 2, 1)

      (0 until precision).map { p => {
        base32 apply (0 until 5).map { i => {
          if (((5 * p) + i) % 2 == 0) {
            val mid = (minLng + maxLng) / 2.0
            if (lng > mid) {
              minLng = mid
              bits(i)
            } else {
              maxLng = mid
              0
            }
          } else {
            val mid = (minLat + maxLat) / 2.0
            if (lat > mid) {
              minLat = mid
              bits(i)
            } else {
              maxLat = mid
              0
            }
          }
        }
        }.reduceLeft((a, b) => a | b)
      }
      }.mkString("")
    })


    // Create direct kafka stream with brokers and topics
    val topics = "driver,customer"
    val topicsSet = topics.split(",").toSet
    val brokers = "localhost:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    //.filter(line => line.toString.contains("READY"))
    val DstreamValues = dstream.map(_._2)//.window(windowInterval, slidingInterval)

    // JDBC writer configuration and persist into DB
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")

    DstreamValues.foreachRDD { rdd =>
      val rawdf = spark.sqlContext.read.json(rdd)

      //adding driver geohash(600M X 1200M) based on lat-lon
      val demandDf = rawdf.filter("Pickup_latlan is not null").withColumn(
        "d_geohash",
        when(
          rawdf.col("Pickup_latlan").isNotNull,
          callUDF("myUDF", rawdf.col("Pickup_latlan"))
        ).otherwise(lit(null))
      ).drop("driver_latlon", "driver_id", "status")
      demandDf.createOrReplaceTempView("demand")
      demandDf.show()
      demandDf.write.mode("append").jdbc("jdbc:mysql://localhost:3306/grab", "demand", connectionProperties)

      //adding customer geohash(600M X 1200M)
      val supplyDf = rawdf.filter("driver_latlon is not null").withColumn(
        "s_geohash",
        when(
          rawdf.col("driver_latlon").isNotNull,
          callUDF("myUDF", rawdf.col("driver_latlon"))
        ).otherwise(lit(null))
      ).drop("Drop_latlan", "Pickup_latlan", "customer_id", "distance", "duration")
      supplyDf.createOrReplaceTempView("supply")
      supplyDf.show()
      supplyDf.write.mode("append").jdbc("jdbc:mysql://localhost:3306/grab", "supply", connectionProperties)

      val supply = spark.sql("select count(distinct driver_id) as supply from supply s,demand d where s.status not in" +
        " ('busy') and s_geohash=d_geohash and SUBSTRING(s.date_time,1,16)=SUBSTRING(d.date_time,1,16)")
      val demand = spark.sql("select count(customer_id) as demand from supply s,demand d where s.status not in ('busy') and s_geohash=d_geohash and SUBSTRING(s.date_time,1,16)=SUBSTRING(d.date_time,1,16)")
      //supply.show()
      //demand.show()
      val driver = supply.filter("supply > 0 ").select("supply").collectAsList()
      val customer = demand.filter("demand > 0").select("demand").collectAsList()
      if (!customer.isEmpty || !driver.isEmpty) {
        val c = customer.get(0).get(0)
        val d = driver.get(0).get(0)
        val ratio = c.toString.toDouble / d.toString.toDouble
        val finalDF = supply.withColumn("demand", lit(c.toString())).withColumn("multiplier", lit(ratio.toString))
        finalDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/grab", "supply_demand_ratio", connectionProperties)
        finalDF.show()
      }
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}

