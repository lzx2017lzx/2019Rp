package com.lzx.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/*
real recommender
 */
case class MongoConfig(val uri:String, val db:String)

case class MovieRecs(mid:Int, recs:Seq[Recommendation])
case class Recommendation(mid:Int, r:Double)

object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("192.168.0.111")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://192.168.0.111:27017/recom"))

}
object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MAX_SIM_MOVIES_NUM = 20



  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[3]",
      "kafka.topic" -> "recom",
      "mongo.uri" -> "mongodb://192.168.0.111:27017/recom",
      "mongo.db" -> "recom"
    )

    //create spark env
    val sparkConf = new SparkConf().setAppName("StreamingRecommender")
      .setMaster(config("spark.cores"))
      .set("spark.executor.memory", "4g")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    implicit val mongConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    //val a = sc.textFile("")
    val simMoviesMatrix = spark
      .read
      .option("uri", mongConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{
        resc =>(resc.mid, resc.recs.map(x=>(x.mid,x.r)).toMap)
      }.collectAsMap()
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)

    val abc = sc.makeRDD(1 to 2)
    abc.map(x=>simMoviesMatrixBroadCast.value.get(1)).count



    val kafkaPara = Map (
      "bootstrap.servers" -> "192.168.0.111:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recomgroup"
    )


    //connect kafka
    val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")),kafkaPara))

    //accept scores streaming
    val reatingStream = kafkaStream.map{
      case msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }


    reatingStream.foreachRDD{
      rdd =>
        rdd.map{
          case(uid, mid, scores, timestamp)=>
            println("get data from kafka")

            //get present most recent scores
            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)
          //get movies closed to p
            val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid,uid, simMoviesMatrixBroadCast.value)

          //calculate priority
          val streamRecs = computeMovieScores(simMoviesMatrixBroadCast.value, userRecentlyRatings, simMovies)

          //save them into mongodb
          saveRecsToMongoDB(uid, streamRecs)
        }.count()
    }

    ssc.start()
    ssc.awaitTermination()

  }

  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig:MongoConfig): Unit = {
    val streamRecsCollect = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streamRecsCollect.findAndRemove(MongoDBObject("uid"->uid))

    streamRecsCollect.insert(MongoDBObject("uid"->uid, "recs"->streamRecs.map(x=>x._1+":"+x._2).mkString("|")))
    println("save to mongoDB")


  }

  def getMovieSimScore(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]], userRatingMovie: Int, topSimMovie:Int):Double = {
    simMovies.get(topSimMovie) match{
      case Some(sim) => sim.get(userRatingMovie) match{
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  def computeMovieScores(simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int, Double]],
                         userRecentlyRatings:Array[(Int, Double)], topSimMovies:Array[Int]
  ):Array[(Int, Double)] = {
    //save every candidate movie and every movie recently
    val score = ArrayBuffer[(Int, Double)]()

    //save increase factor
    val increMap= mutable.HashMap[Int, Int]()
    //save decrease factor
    val decreMap = mutable.HashMap[Int, Int]()
    for(topSimMovies <- topSimMovies;userRecentlyRatings <- userRecentlyRatings){
      val simScore = getMovieSimScore(simMovies, userRecentlyRatings._1, topSimMovies)
      if(simScore > 0.6){
        score += ((topSimMovies, simScore * userRecentlyRatings._2))

        if(userRecentlyRatings._2 > 3){
          //increase factor works
          increMap(topSimMovies) = increMap.getOrDefault(topSimMovies,0) + 1
        }else{
          //decrease factor works
          decreMap(topSimMovies) = decreMap.getOrDefault(topSimMovies, 0) + 1
        }
      }
    }
      score.groupBy(_._1).map{
        case (mid,sims) =>
          (mid, sims.map(_._2).sum/sims.length + log(increMap(mid)) - log(decreMap(mid)))
      }.toArray



    }

  def log(m:Int):Double ={
    math.log(m)/math.log(2)
  }
/*
get present closest m times movie scores
 */
  def getUserRecentlyRating(num:Int, uid:Int, jedis:Jedis):Array[(Int, Double)] = {
    //get num scores in user queue
    jedis.lrange("uid:"+uid.toString,0,num).map{
      item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt,attr(1).trim.toDouble)
    }.toArray
  }


  def getTopSimMovies(num:Int, mid:Int, uid:Int, simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int, Double]])(implicit mongoConfig:MongoConfig):Array[Int] = {
//from broadcast variable simi get all similarity
    val allSimMovies = simMovies.get(mid).get.toArray

    //get movie user watched
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
        .find(MongoDBObject("uid"->uid)).toArray.map{
      item =>
        item.get("mid").toString.toInt
    }

    //filter scored movie and sorting out
   allSimMovies.filter(x=> !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x=>x._1)


  }



}
