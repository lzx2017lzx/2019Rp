package dayone

import scala.collection.mutable.Queue

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

/*
 * RDD queue
 */
object RDDqueue {
  def main(args:Array[String]):Unit = {
    
    val conf = new SparkConf().setAppName("RDD Queue stream").setMaster("local[2]")
    
    val ssc = new StreamingContext(conf, Seconds(1))
    
    val rddQueue = new Queue[RDD[Int]]()
    
    for(i <- 1 to 3){
      rddQueue += ssc.sparkContext.makeRDD(1 to 10)
     
      Thread.sleep(3000)
    }
    
    //create dstream
    val inputDStream = ssc.queueStream(rddQueue)
    
    val result = inputDStream.map(x => (x, x*2))
    
    result.print()
    
    //start
    ssc.start()
    
    ssc.awaitTermination()
    }
}