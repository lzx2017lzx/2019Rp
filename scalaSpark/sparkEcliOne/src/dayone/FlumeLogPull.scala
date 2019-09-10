package dayone

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.storage.StorageLevel

/*
 * 
 */
object FlumeLogPull {
  def main(args:Array[String]):Unit ={
       Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
		
		val conf = new SparkConf().setAppName("FluemLogPull").setMaster("local[2]")
		
		val ssc = new StreamingContext(conf, Seconds(10))
       
    //create pull stream
    val flumeStream = FlumeUtils.createPollingStream(ssc, "192.168.0.111", 12345)
    
    val line = flumeStream.map(e =>{
      new String(e.event.getBody.array)
    })
    
    line.print()
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}