package com.lzx.load

import java.net.InetAddress

import org.apache.spark.sql.SparkSession
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
//import data into system
object DataLoader {
  // table in mongodb is called collection
  val MOVIES_COLLECTION_NAME = "Movie"
  val RATING_COLLECTION_NAME = "Rating"
  val TAGS_COLLECTION_NAME = "Tag"

  val ES_TAG_TYPE_NAME = "Movie"
  val ES_HOST_PORT_REGEX = "(.+):(\\d+)".r

  def main(args:Array[String]):Unit = {
    val DATAFILE_MOVIES = "E:\\lzx\\recommander\\reco_data\\reco_data\\small\\movies.csv"
    val DATAFILE_RATINGS = "E:\\lzx\\recommander\\reco_data\\reco_data\\small\\ratings.csv"
    val DATAFILE_TAGS = "E:\\lzx\\recommander\\reco_data\\reco_data\\small\\tags.csv"


    // create golbal set
    val params = scala.collection.mutable.Map[String,Any]()
    params += "spark.cores" -> "local[2]"
    params += "mongo.uri" -> "mongodb://192.168.0.111:27017/recom"
    params += "mongo.db" -> "recom"
    params += "es.httpHosts" -> "192.168.0.111:9200"
    params += "es.transportHosts" -> "192.168.0.111:9300"
    params += "es.index" -> "recom"
    params += "es.cluster.name" -> "my-application"



    implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String], params("mongo.db").asInstanceOf[String])
    implicit val eSConf = new ESConfig(params("es.httpHosts").asInstanceOf[String],
      params("es.transportHosts").asInstanceOf[String],params("es.index").asInstanceOf[String],
      params("es.cluster.name").asInstanceOf[String]
    )

    //claim spark environment
    val config = new SparkConf().setAppName("DataLoader")
      .setMaster(params("spark.cores").asInstanceOf[String])

    val spark = SparkSession.builder().config(config).getOrCreate()

    // load data set:Movies Rating Tag
    val movieRDD = spark.sparkContext.textFile(DATAFILE_MOVIES)

    val ratingRDD = spark.sparkContext.textFile(DATAFILE_RATINGS)


    val tagRDD = spark.sparkContext.textFile(DATAFILE_TAGS)

    import spark.implicits._
    //transform RDD to DataFrame
    val movieDF = movieRDD.map(line => {
      val x = line.split("\\^")
      Movie(x(0).trim.toInt,x(1).trim,x(2).trim,x(3).trim,x(4).trim,x(5).trim,x(6).trim,x(7).trim,x(8).trim,x(9).trim)
    }).toDF()

    val ratingDF = ratingRDD.map(line => {
      val x = line.split(",")
      Rating(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toDouble, x(3).toInt)
    }).toDF()

    val tagDF = tagRDD.map(line => {
      val x = line.split(",")
      Tag(x(0).toInt, x(1).toInt, x(2), x(3).toInt)
    }).toDF()

    //save data into mongodb
    //storeDataInMongo(movieDF, ratingDF, tagDF)

    movieDF.cache()
    tagDF.cache()

    import org.apache.spark.sql.functions._
    //gather tagDF and movieid, combine tag
    val tagCollectDF = tagDF.groupBy($"mid").agg(concat_ws("|", collect_set($"tag")).as("tags"))


    //combime tags and movie, produce new data set
    val esMovieDF = movieDF.join(tagCollectDF, Seq("mid", "mid"), "left")
        .select("mid", "name", "descri", "timelong", "issue", "shoot", "language","genres","actors","directors", "tags")

    //esMovieDF.show(20)
    //save data into ES
    storeDataInES(esMovieDF)

    //erase cache
    movieDF.unpersist()
    tagDF.unpersist()

    spark.close()
  }

  private def storeDataInMongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //create connection between mongodb
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).dropCollection()
    mongoClient(mongoConfig.db)(RATING_COLLECTION_NAME).dropCollection()
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).dropCollection()

    //write movie data set into mongodb
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIES_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", RATING_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", TAGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //create index
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATING_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(RATING_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))


    //close connection
    mongoClient.close()


  }

  private def storeDataInES(esMovieDF: DataFrame)(implicit esConfig:ESConfig): Unit = {

    val indexName = esConfig.index
    val settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()

    val esClient = new PreBuiltTransportClient(settings)

    esConfig.transportHosts.split(",")
        .foreach{
          case ES_HOST_PORT_REGEX(host:String, port:String) =>
            esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
        }

    //judge wether index exist or not
    if(esClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()){
    //delete index
      esClient.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
    }

    //create index
    esClient.admin().indices().create(new CreateIndexRequest(indexName)).actionGet()

    val movieOptions = Map("es.nodes" -> esConfig.httpHosts,//es.node 这种写法是错误的
      "es.http.timeout" -> "800m",
      "es.mapping.id" -> "mid"
    )

    val movieTypeName = s"$indexName/$ES_TAG_TYPE_NAME"
    esMovieDF
      .write.options(movieOptions)
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(movieTypeName)
  }
}
