package com.atguigu.dataloader

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
 * Action|Drama|Romance|War ^       电影的类型
 * Liam Neeson|Jessica Lange...     电影的演员
 * Michael Caton-Jones              电影的导演
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

// 数据的主加载服务
object DataLoader {

    // 程序的入口
    def main(args: Array[String]): Unit = {

        // 需要创建SparkConfig配置
        val sparkConf = null;

        // 创建一个SparkSession
        val spark = null;

        // 将movies, ratings, tags数据集加载进来
        val movieRDD = null;
        val ratingRDD = null;
        val tagRDD = null;

        // 需要将数据保存到MongoDB中
        storeDataInMongoDB()

        // 需要将数据保存到es中
        storeDataInES()

        // 关闭Spark
        // spark.stop
    }

    // 将数据加载到MongoDB中的方法
    def storeDataInMongoDB(): Unit = {

    }

    // 将数据保存到ES中的方法
    def storeDataInES(): Unit = {

    }

}
