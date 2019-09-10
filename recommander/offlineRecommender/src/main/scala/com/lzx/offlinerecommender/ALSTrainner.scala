package com.lzx.offlinerecommender

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*

 */
object ALSTrainner {


  def main(args: Array[String]): Unit = {
    val conf = Map(
      "spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://192.168.0.111:27017/recom",
      "mongo.db" -> "recom"
    )

    //create spark conf
    val sparkConf = new SparkConf().setAppName("ALS Trainner").setMaster(conf("spark.cores"))


    //create spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //load scores data
    val mongoConfig = MongoConfig(conf("mongo.uri"), conf("mongo.db"))
    import spark.implicits._

    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", OfflineRecommender.MONGODB_TATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.scores)).cache()

    //output excellent argument
    adjustALSParams(ratingRDD)

    spark.close()
  }


  def adjustALSParams(ratingRDD: RDD[Rating]): Unit = {
    val result = for(rank <- Array(30, 40, 50, 60, 70);lambda <- Array(1, 0.1, 0.01))
      yield{
        val model = ALS.train(ratingRDD, rank, 5, lambda)
        //gain model
        val rmse = getRmse(model, ratingRDD)
        (rank, lambda, rmse)
      }

    println(result.sortBy(_._3).head)
    }


  def getRmse(model: MatrixFactorizationModel, ratingRDD: RDD[Rating]) = {
    //construct usersProducts RDD
    val userMovies = ratingRDD.map(item => (item.user,item.product))
    val predictRating = model.predict(userMovies)

    val real = ratingRDD.map(item => ((item.user,item.product),item.rating))
    val predict = predictRating.map(item => ((item.user,item.product),item.rating))

    //import spark.implicits._
    sqrt(
      real.join(predict)
        .map{
          case ((uid, mid), (real,pre)) =>
            //calculate difference between real value and predict value
          val err = real - pre
            err * err
        }.mean())
  }
}
