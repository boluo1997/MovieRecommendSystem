package com.atguigu.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date

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
 * MongoDB connection configuration
 *
 * @param uri MongoDB的连接
 * @param db  MongoDB要操作的数据库
 */
case class MongoConfig(val uri: String, val db: String)

/**
 * 推荐电影
 *
 * @param mid   电影推荐的id
 * @param score 电影推荐的评分
 */
case class Recommendation(mid: Int, score: Double)

/**
 * 电影类别推荐
 *
 * @param genres 电影类别
 * @param recs   top10的电影集合
 */
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender {

    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_COLLECTION = "Movie"

    // 统计的表的名称
    val RATE_MORE_MOVIES = "RateMoreMovies"
    val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
    val AVERAGE_MOVIES = "AverageMovies"
    val GENRES_TOP_MOVIES = "GenresTopMovies"


    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://192.168.177.128:27017/recommender",
            "mongo.db" -> "recommender"
        )

        // 创建SparkConf配置
        val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

        // 创建SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        // 加入隐式转换
        import spark.implicits._

        // 数据加载进来
        val ratingDF = spark
            .read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Rating]
            .toDF()

        val movieDF = spark
            .read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Movie]
            .toDF()

        ratingDF.createOrReplaceTempView("ratings")

        // 统计所有历史数据中, 每个电影的评分数
        // 数据结构 -> mid, count
        val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")

        rateMoreMoviesDF
            .write
            .option("uri", mongoConfig.uri)
            .option("collection", RATE_MORE_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        // 统计以月为单位, 每个电影的评分数
        // 数据结构 -> mid, count, time
        val simpleDateFormat = new SimpleDateFormat("yyyyMM")

        // 将timestamp转换成年月格式
        spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

        // 将原来的Rating数据集中的时间转换成年月的格式
        val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")

        ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
        val rateMoreRecentlyMovies = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid")

        rateMoreRecentlyMovies
            .write
            .option("uri", mongoConfig.uri)
            .option("collection", RATE_MORE_RECENTLY_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        // 统计每个电影的平均评分
        val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")

        averageMoviesDF
            .write
            .option("uri", mongoConfig.uri)
            .option("collection", AVERAGE_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        // 统计每种电影类别中, 评分最高的top10

        // join数据集averageMoviesDF和movieWithScore
        val movieWithScore = movieDF.join(averageMoviesDF, Seq("mid"))

        // 电影种类
        val genres =
            List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign",
                "History", "Horror", "Music", "Mystery"
                , "Romance", "Science", "Tv", "Thriller", "War", "Western")

        // genres转换为RDD
        val genresRDD = spark.sparkContext.makeRDD(genres)

        // movieWithScore与genres进行笛卡尔积
        val genresTopMovies = genresRDD.cartesian(movieWithScore.rdd)
            .filter {
                // 过滤电影类别不匹配的电影
                case (genres, row) =>
                    row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
            }
            .map {
                // 减少数据集的数据量
                case (genres, row) =>
                    (genres, ((row.getAs[Int]("mid")), row.getAs[Double]("avg")))
            }.groupByKey() // 将数据集中的类别相同的电影进行聚合
            .map {
                // 通过评分大小进行降序排序
                case (genres, items) => GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2)
                    .take(10).map(item => Recommendation(item._1, item._2)))
            }.toDF()

        genresTopMovies
            .write
            .option("uri", mongoConfig.uri)
            .option("collection", GENRES_TOP_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        // 关闭spark
        spark.stop()
    }
}
