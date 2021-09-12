package com.atguigu.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

object ConnHelper extends Serializable {
    lazy val jedis = new Jedis("192.168.177.128")
    lazy val mongoClient = MongoClient(MongoClientURI("mongodb://192.168.177.128:27017/recommender"))
}

case class MongoConfig(uri: String, db: String)

// 推荐
case class Recommendation(rid: Int, r: Double)

// 用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 电影的相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object StreamingRecommender {

    val MAX_USER_RATINGS_NUM = 20
    val MAX_SIM_MOVIES_NUM = 20
    val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

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

        implicit var mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
        import spark.implicits._

        // ************************************************ 广播电影相似度矩阵

        // 转换成为 Map[Int, Map[Int, Double]]
        val simMoviesMatrix = spark
            .read
            .option("uri", config("mongo.uri"))
            .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[MovieRecs]
            .rdd
            .map { recs =>
                (recs.mid, recs.recs.map(x => (x.rid, x.r)).toMap)
            }.collectAsMap()

        val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)

        // ************************************************

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

                // 获取当前用户最近的M次电影评分
                val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

                // 获取电影P最相似的K个电影
                val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadCast.value)

                // 计算待选电影的推荐优先级

                // 将数据保存到MongoDB

            }.count()
        }

        // 启动Streaming程序
        ssc.start()
        ssc.awaitTermination()
    }

    /**
     * 获取当前电影K个相似的电影
     *
     * @param num        相似电影的数量
     * @param mid        当前电影ID
     * @param uid        当前用户ID
     * @param simMovies  电影相似度矩阵的广播变量值
     * @param mongConfig mongoDB的配置
     * @return
     */
    def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongConfig: MongoConfig): Array[Int] = {

        // 从广播变量的电影相似度矩阵中获得当前电影所有的相似电影
        val allSimMovies = simMovies.get(mid).get.toArray

        // 获取用户已经观看过的电影
        val ratingExist = ConnHelper.mongoClient(mongConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject("uid" -> uid)).toArray.map { item =>
            item.get("mid").toString.toInt
        }

        // 过滤掉已经评分过的电影, 并排序输出
        allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)
    }


    /**
     * 获取当前用户最近的M次电影评分
     *
     * @param num 评分的个数
     * @param uid 哪个用户的评分
     * @return
     */
    def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {

        // 从用户的队列中取出num个评论
        jedis.lrange("uid:" + uid.toString, 0, num).map { item =>
            var attr = item.split(":")
            (attr(0).trim.toInt, attr(1).trim.toDouble)
        }.toArray

    }

}
