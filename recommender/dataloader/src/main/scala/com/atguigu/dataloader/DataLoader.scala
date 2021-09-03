package com.atguigu.dataloader

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import java.net.InetAddress

/**
 * Movies数据集, 数据集字段通过 ^分割
 *
 * 151^                             电影ID
 * Rob Roy (1995)^                  电影名称
 * In the highlands ...             电影描述
 * 139 minutes^                     电影时长
 * August 26, 1997^                 电影发行日期
 * 1995^                            电影拍摄日期
 * English ^                        电影的语言
 * Action|Drama|Romance|War         电影的类型
 * Liam Neeson|Jessica Lange...     电影的演员
 * Michael Caton-Jones              电影的导演
 *
 * 处理成这种形式:
 * Mid, tag1|tag2|tag3|...
 */

case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String,
                 val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

/**
 * Rating数据集, 用户对于电影的评分, 用,分割
 *
 * 1,                   用户ID
 * 31,                  电影ID
 * 2.5,                 用户对于电影的评分
 * 1260759144           用户对于电影评分的时间
 */

case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

/**
 * Tags数据集, 用户对于电影的标签数据集, 用,分割
 *
 * 15,              用户ID
 * 1955,            电影ID
 * dentist,         标签的具体内容
 * 1193435061       用户对电影打标签的时间
 */

case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)

/**
 * MongoDB connection configuration
 *
 * @param uri MongoDB的连接
 * @param db  MongoDB要操作的数据库
 */
case class MongoConfig(val uri: String, val db: String)

/**
 * elasticsearch connection configuration
 *
 * @param httpHosts      Http的主机列表, 以,分割
 * @param transportHosts Transport主机列表, 以,分割
 * @param index          需要操作的索引
 * @param clusterName    ES集群的名称
 */
case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String, val clusterName: String)

// 数据的主加载服务
object DataLoader {

    val MOVIE_DATA_PATH = "D:\\IDEA_Projects\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\movies.csv"
    val RATING_DATA_PATH = "D:\\IDEA_Projects\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\ratings.csv"
    val TAG_DATA_PATH = "D:\\IDEA_Projects\\MovieRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\tags.csv"

    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"

    val ES_MOVIE_INDEX = "Movie"

    // 程序的入口
    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://192.168.177.128:27017/recommender",
            "mongo.db" -> "recommender",
            "es.httpHosts" -> "192.168.177.128:9200",
            "es.transportHosts" -> "192.168.177.128:9300",
            "es.index" -> "recommender",
            "es.cluster.name" -> "es-cluster"
        )

        // 需要创建SparkConfig配置
        val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config.get("spark.cores").get)

        // 创建一个SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._

        // 将movies, ratings, tags数据集加载进来
        val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
        val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
        val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)

        val movieDF = movieRDD.map(item => {
            val attr = item.split("\\^")
            Movie(
                attr(0).toInt,
                attr(1).trim,
                attr(2).trim,
                attr(3).trim,
                attr(4).trim,
                attr(5).trim,
                attr(6).trim,
                attr(7).trim,
                attr(8).trim,
                attr(9).trim
            )
        }).toDF()

        val ratingDF = ratingRDD.map(item => {
            val attr = item.split(",")
            Rating(
                attr(0).toInt,
                attr(1).toInt,
                attr(2).toDouble,
                attr(3).toInt
            )
        }).toDF()

        val tagDF = tagRDD.map(item => {
            val attr = item.split(",")
            Tag(
                attr(0).toInt,
                attr(1).toInt,
                attr(2).trim,
                attr(3).toInt
            )
        }).toDF()

        implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)

        // 需要将数据保存到MongoDB中
        // storeDataInMongoDB(movieDF, ratingDF, tagDF)

        /**
         * 首先需要将 Tag数据集进行处理, 处理后的形式: Mid, tag1|tag2|tag3|...
         * 然后需要将处理后的Tag数据与Movie数据融合, 产生新的Movie数据
         * 最后将新的Movie数据保存到ES中
         */
        import org.apache.spark.sql.functions._

        /**
         * 以|为中间线, 聚合Tag
         * MID, Tags
         * 1    tag1|tag2|tag3...
         */
        val newTag = tagDF.groupBy($"mid")
            .agg(concat_ws("|", collect_set($"tag")).as("tags"))
            .select("mid", "tags")

        val movieWithTagsDF = movieDF.join(newTag, Seq("mid", "mid"), "left")

        // 声明了一个es配置的隐式参数
        implicit val esConfig = ESConfig(
            config.get("es.httpHosts").get,
            config.get("es.transportHosts").get,
            config.get("es.index").get,
            config.get("es.cluster.name").get)

        // 需要将数据保存到es中
        storeDataInES(movieWithTagsDF)(esConfig)

        // 关闭Spark
        spark.stop()
    }

    // 将数据加载到MongoDB中的方法
    def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

        // 新建一个到MongoDB的连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

        // 如果MongoDB中有对应的数据库, 应该删除
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

        // 将当前数据写入到MongoDB
        movieDF.write
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        ratingDF.write
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_RATING_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        tagDF.write
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_TAG_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        // 对数据表建索引
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))

        // 关闭MongoDB连接
        mongoClient.close()
    }

    // 将数据保存到ES中的方法
    def storeDataInES(movieDF: DataFrame)(implicit esConfig: ESConfig): Unit = {

        // 新建一个配置
        val settings: Settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()

        // 新建一个es的客户端
        val esClient = new PreBuiltTransportClient(settings)

        // 需要将transportHost添加到esClient中
        val REGEX_HOST_PORT = "(.+):(\\d+)".r
        esConfig.transportHosts.split(",").foreach {
            case REGEX_HOST_PORT(host: String, port: String) => {
                esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
            }
        }

        // 需要清除掉es中遗留的数据
        if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists) {
            esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
        }
        esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

        // 将数据写入到es中
        movieDF.write
            .option("es.nodes", esConfig.httpHosts)
            .option("es.http.timeout", "100m")
            .option("es.mapping.id", "mid")
            .mode("overwrite")
            .format("org.elasticsearch.spark.sql")
            .save(esConfig.index + "/" + ES_MOVIE_INDEX)

    }

}
