package com.lzx.offlinerecommender

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

case class Movie(val mid:Int, val name:String, val descri:String,
                 val timelong:String, val issue:String, val shoot:String,
                 val language:String, val genres:String, val actors:String,
                 val directors:String)

case class MovieRating(val uid:Int, val mid:Int, val scores:Double, val timestamp:Int)

case class Tag(val uid:Int, val mid:Int, val tag:String, val timestamp: Int)

case class MongoConfig(val uri:String, val db:String)

case class ESConfig(val httpHosts:String,val transportHosts:String,val index:String,val clusterName:String)

case class Recommendation(mid:Int, r:Double)

case class GenresRecommendation(genres:String, recs:Seq[Recommendation])

case class UserRecs(uid:Int, recs:Seq[Recommendation])

case class MovieRecs(mid:Int, recs:Seq[Recommendation])

/**
  * use ALS better
  */
object OfflineRecommender {
  val MONGODB_TATING_COLLECTION="Rating"
  val MONGODB_MOVIE_COLLECTION="Movie"

  val MONGODB_USER_RECS="UserRecs"
  val USER_MAX_RECOMMENDATIONG = 10
  val MONGO_MOVIE_RECS = "MovieRecs"

  def main(args: Array[String]): Unit = {
    val conf = Map(
      "spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://192.168.0.111:27017/recom",
      "mongo.db" -> "recom"
    )
    //create spark env
    val sparkConf = new SparkConf().setAppName("Offline Recommender")
      .setMaster(conf("spark.cores"))
      .set("spark.executor.memory", "6G")
      .set("spark.driver.memory","2G")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    //gain mongodb data
    val mongoConfig = MongoConfig(conf("mongo.uri"), conf("mongo.db"))

    import spark.implicits._

    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid,rating.mid,rating.scores)).cache

    val movieRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache()


    //traing ALS model
    /**
      * send four argument
      * traing data
      * Rating object collection,contain:user ID、commody ID、preference
      *
      * rank
      * charactoristic dimension:50
      *
      * iterations
      * times of iterations
      *
      * lambda:
      * 0.01
      *
      */

    //form train data
    val trainData = ratingRDD.map(x => Rating(x._1,x._2,x._3))

    val (rank,iterations,lambda) = (50, 5, 0.01)
    val model = ALS.train(trainData,rank,iterations,lambda)

    //calculate user recommend matrix
    val userRDD = ratingRDD.map(_._1).distinct().cache()

    val userMovies = userRDD.cartesian(movieRDD)

    val preRatings = model.predict(userMovies)

    val userRecs = preRatings
      .filter(_.rating > 0)
      .map(rating => (rating.user,(rating.product, rating.rating)))
      .groupByKey()
      .map{
        case (uid, recs) =>
          UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATIONG)
          .map(x=>Recommendation(x._1,x._2)))
      }.toDF

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //calculate movie similary matrix




    //gain movie similary matrix
    val movieFeatures = model.productFeatures.map{
      case(mid, features) => (mid, new DoubleMatrix(features))
    }

    val movieRecs = movieFeatures.cartesian(movieFeatures)
        .filter{
          case (a,b) => a._1 != b._1
        }
        .map{
          case (a,b) =>
            val simScore = this.consinSim(a._2,b._2)
            (a._1,(b._1,simScore))
        }
      .filter(_._2._2 > 0.6)
      .groupByKey()
        .map{
          case(mid, items) =>
            MovieRecs(mid, items.toList.map(x=>Recommendation(x._1,x._2)))
        }.toDF

    movieRecs.write
        .option("uri", mongoConfig.uri)
        .option("collection", MONGO_MOVIE_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

    userRDD.unpersist()
    movieRDD.unpersist()
    ratingRDD.unpersist()

    spark.close()
  }

  //calculate cosin similarity
  def consinSim(movie1:DoubleMatrix, movie2:DoubleMatrix):Double = {
    movie1.dot(movie2)/(movie1.norm2()*movie2.norm2())
  }
}
