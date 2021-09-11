package com.atguigu.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingRecommender {

    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://192.168.177.128:27017/recommender",
            "mongo.db" -> "recommender",
            "kafka.topic" -> "recommender"
        )

        // 创建sparkConf
        val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))

        // 创建spark
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        val sc = spark.sparkContext
        val ssc = new StreamingContext(sc, Seconds(2))

        // 创建到Kafka连接
        val kafkaPara = Map(
            "bootstrap.servers" -> "192.168.177.128:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "recommender",
            "auto.offset.reset" -> "latest"
        )

        val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))

        // UID|MID|SCORE|TIMESTAMP
        // 产生评分流
        val ratingStream = kafkaStream.map {
            case msg =>
                var attr = msg.value().split("\\|")
                (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
        }

        ratingStream.foreachRDD { rdd =>
            rdd.map { case (uid, mid, score, timestamp) =>
                println(">>>>>>>>>>>")
            }.count()
        }

        // 启动Streaming程序
        ssc.start()
        ssc.awaitTermination()
    }


}
