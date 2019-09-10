package dayone

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import java.util.logging.Level
import java.util.logging.Logger

/*
 * develop myself streaming program
 * 
 * 
 */
object MyNetworkWordCount {
  def main(args:Array[String]):Unit={
    //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    //create config and streaming context
    val conf = new SparkConf().setAppName("my network word count").setMaster("local[2]")
    
    val streamContext = new StreamingContext(conf, Seconds(3))
    
    //create dstream
    val lines = streamContext.socketTextStream("192.168.0.111", 1234, StorageLevel.MEMORY_ONLY)
    
    val words = lines.flatMap(_.split(" "))
    
    //val wordCount = words.map((_,1)).reduceByKey(_+_)
    val wordCount = words.transform(x =>x.map(x=>(x,1)))
    
    wordCount.print()
    
    //
    streamContext.start()
    
    streamContext.awaitTermination()
  }
}