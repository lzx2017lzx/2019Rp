package com.lzx.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StaticsApp extends App{
  val RATINGS_COLLECTION_NAME = "Rating"
  val MOVIE_COLLECTION_NAME = "Movie"


  val params = scala.collection.mutable.Map[String,Any]()
  params += "spark.cores" -> "local[2]"
  params += "mongo.uri" -> "mongodb://192.168.0.111:27017/recom"
  params += "mongo.db" -> "recom"

  val conf = new SparkConf().setAppName("StaticsApp Recommaender")
    .setMaster(params("spark.cores").asInstanceOf[String])

  val spark = SparkSession.builder().config(conf).getOrCreate()

  //operate mongodb
  implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String], params("mongo.db").asInstanceOf[String])

  import spark.implicits._
  //read mongodb data
  val ratings = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", RATINGS_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Rating].cache

  val movies = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", MOVIE_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie].cache

  ratings.createOrReplaceTempView("ratings")
  //statics

  //staticsRecommender.rateMore(spark)
  //staticsRecommender.rateMoreRecently(spark)
  staticsRecommender.genresTop10(spark)(movies)
  ratings.unpersist()
  movies.unpersist()

  spark.close()

}
