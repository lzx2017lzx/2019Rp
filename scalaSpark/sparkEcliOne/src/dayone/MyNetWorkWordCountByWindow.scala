package dayone

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

/*
 * window operation demo
 */
object MyNetWorkWordCountByWindow {
  def main(args:Array[String]):Unit={
    // init spark object
    val conf = new SparkConf().setAppName("my network word count by window").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(1))
    
    
    // create dstream
    val lines = ssc.socketTextStream("192.168.0.111", 1234, StorageLevel.MEMORY_ONLY)
    
    // words count
    val words = lines.flatMap(_.split(" ")).map((_,1))
   
    
    // window ope
     val result =  words.reduceByKeyAndWindow((x:Int,y:Int)=>(x+y),Seconds(30), Seconds(10))
    
      result.print()
    // start spark streaming
     
     ssc.start()
     
     ssc.awaitTermination()
  }
}