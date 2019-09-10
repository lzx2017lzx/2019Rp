package dayone

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * spark streaming file flow
 */
object FileStream {
  def main(args:Array[String]):Unit = {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //spark env
    val conf = new SparkConf().setAppName("File Stream").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    //listening directory
    val lines = ssc.textFileStream("E:\\sparkTest\\temp_file\\lala.txt");
    lines.print()
    
    //start
    ssc.start()
    ssc.awaitTermination()
  }
}