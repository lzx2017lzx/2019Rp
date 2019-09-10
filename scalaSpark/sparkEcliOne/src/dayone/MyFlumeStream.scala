package dayone

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.flume.FlumeUtils


/*
 * spark streaming flume data handled
 */
object MyFlumeStream {
  def main(args:Array[String]):Unit = {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
		
    val conf = new SparkConf().setAppName("My Flume Stream").setMaster("local[2]")
    
    val ssc = new StreamingContext(conf, Seconds(3))
    
    //accept flume data
    //create flumeEvent
    val flumeEventDStream = FlumeUtils.createStream(ssc, "192.168.0.104", 1234)
    
    val lineDstream = flumeEventDStream.map(e =>{
      new String(e.event.getBody.array)
    })
    
    lineDstream.print()
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}