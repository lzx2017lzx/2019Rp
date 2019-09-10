package dayone

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession

/*
 * spark streaming aggragate spark sql
 */
object MyNetWorkWordCountWithSQL {
  def main(args:Array[String]):Unit = {
    //create spark env
    val conf = new SparkConf().setAppName("my net work word count with sql").setMaster("local[2]")
    
    val ssc = new StreamingContext(conf, Seconds(3))
       
    
    //create dstream
    
    val lines = ssc.socketTextStream("192.168.0.111", 1234, StorageLevel.MEMORY_ONLY)
    
    //word count
    val words = lines.flatMap(_.split(" "))
    
    //aggragate spark sql
    words.foreachRDD(rdd =>{
    val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    
    import spark.implicits._
    val df1 = rdd.toDF("word")
    
    //create view
    df1.createOrReplaceTempView("words")
    
    //execute sql
    spark.sql("select word,count(1) from words group by word").show()
    
    })
    
    
    
    //start env
    ssc.start()
    
    ssc.awaitTermination()
    
  }
}