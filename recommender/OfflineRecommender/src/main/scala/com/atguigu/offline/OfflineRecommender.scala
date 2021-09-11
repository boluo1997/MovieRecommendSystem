package com.atguigu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

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
case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

/**
 * MongoDB connection configuration
 *
 * @param uri MongoDB的连接
 * @param db  MongoDB要操作的数据库
 */
case class MongoConfig(val uri: String, val db: String)

// 推荐
case class Recommendation(rid: Int, r: Double)

// 用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 电影的相似度
case class MovieRecs(uid: Int, recs: Seq[Recommendation])

object OfflineRecommender {

    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_COLLECTION = "Movie"

    val USER_MAX_RECOMMENDATION = 20

    val USER_RECS = "UserRecs"
    val MOVIE_RECS = "MovieRecs"

    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://192.168.177.128:27017/recommender",
            "mongo.db" -> "recommender"
        )

        // 创建sparkConf配置
        val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
            .set("spark.executor.memory", "6G").set("spark.driver.memory", "3G")
        // 创建sparkSession

        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // 创建mongoConfig
        val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        import spark.implicits._

        // 读取mongoDB中的业务数据
        val ratingRDD = spark
            .read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[MovieRating]
            .rdd
            .map(rating => (rating.uid, rating.mid, rating.score))
            .cache()

        // 用户的数据集 RDD[Int]
        val userRDD = ratingRDD.map(_._1).distinct()

        // 电影的数据集 RDD[Int]
        val movieRDD = spark
            .read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Movie]
            .rdd
            .map(_.mid)

        // 创建训练数据集
        val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
        val (rank, iterations, lambda) = (50, 5, 0.01)

        // 训练ALS模型
        val model = ALS.train(trainData, rank, iterations, lambda)

        // 计算用户推荐矩阵

        // 需要构造一个usersProducts RDD[(Int, Int)]
        val userMovies = userRDD.cartesian(movieRDD)

        // 预测值
        val preRatings = model.predict(userMovies)
        val userRecs = preRatings
            .filter(x => x.rating > 0.6)
            .map(rating => (rating.user, (rating.product, rating.rating)))
            .groupByKey()
            .map {
                case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2) // 降序排序
                    .take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
            }.toDF()

        // 写入mongoDB数据库
        userRecs.write
            .option("uri", mongoConfig.uri)
            .option("collection", USER_RECS)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        // 计算电影的相似度矩阵
        // 获取电影的特征矩阵
        val movieFeatures = model.productFeatures.map {
            case (mid, features) => (mid, new DoubleMatrix(features))
        }
        // 过滤
        val movieRecs = movieFeatures.cartesian(movieFeatures)
            .filter {
                case (x, y) => x._1 != y._1
            }
            .map {
                case (x, y) =>
                    val simScore = consinSim(x._2, y._2)
                    (x._1, (y._1, simScore))
            }
            .filter(_._2._2 > 0.6)
            .groupByKey()
            .map {
                case (mid, item) => MovieRecs(mid, item.toList.map(x => Recommendation(x._1, x._2)))
            }.toDF()

        // 写入数据库
        movieRecs
            .write
            .option("uri", mongoConfig.uri)
            .option("collection", MOVIE_RECS)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        spark.close()
    }


    def consinSim(x: DoubleMatrix, y: DoubleMatrix): Double = {
        x.dot(y) / (x.norm2() * y.norm2())
    }

}
