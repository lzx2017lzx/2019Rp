package com.lzx.test

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Dataset, SparkSession}

object staticsRecommender{
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_MOVIES_RECENTLY = "RateMoreMoviesRecently"
  val GENRES_TOP_MOVIES = "GenresTopMovies"
  val AVERAGE_MOVIES_SCORE = "AverageMoviesScore"
  //statics the most movies
  def rateMore(spark:SparkSession)(implicit mongoConfig: MongoConfig):Unit = {
    val rateMoreDF = spark.sql("select mid, count(1) as count from ratings group by mid order by count desc")

    rateMoreDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  //statictic hot movies
  def rateMoreRecently(spark:SparkSession)(implicit mongoConfig: MongoConfig):Unit = {

    /*
    udf
    timestamp -> "201907"
     */
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate", (x:Long)=>simpleDateFormat.format(new Date(x*1000L)).toLong)

    val yearMonthOfRatings = spark.sql("select mid,uid,scores,changeDate(timestamp) as yearmonth from ratings")
    yearMonthOfRatings.createOrReplaceTempView("yearmonth")
    spark.sql("select mid, count(1) as count, yearmonth from yearmonth group by yearmonth,mid order by yearmonth")
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MORE_MOVIES_RECENTLY)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }

  //statistic average score most top 10
  def genresTop10(spark:SparkSession)(movies:Dataset[Movie])(implicit mongoConfig: MongoConfig):Unit = {
    //define all movie type
    var genres = List("Action","Adventure","Animation","Comedy","Ccrime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    val averageMovieScoreDF = spark.sql("select mid,avg(scores) as avg from ratings group by mid").cache()

    //statistic type the highest 10 movies
    val moviesWithScoreDF = movies.join(averageMovieScoreDF,Seq("mid", "mid")).select("mid", "avg","genres").cache()

    val genresRDD = spark.sparkContext.makeRDD(genres)

    import spark.implicits._

    val genresTopMovies = genresRDD.cartesian(moviesWithScoreDF.rdd).filter{
      case(genres,row) => {
          row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }
    }.map{
      case(genres,row) => {
        (genres,( row.getAs[Int]("mid"),row.getAs[Double]("avg")))
      }
    }.groupByKey().map{
      case(genres, items) => {
        GenresRecommendation(genres, items.toList.sortWith(_._2>_._2).take(10).map(x=>Recommendation(x._1, x._2)))
      }
    }.toDF

    genresTopMovies
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    averageMovieScoreDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", AVERAGE_MOVIES_SCORE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
