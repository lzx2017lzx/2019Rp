package dayone

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object MyTotalNetWorkWordCount {
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("My Total Network Word Count").setMaster("local[2]")
    
    val ssc = new StreamingContext(conf,Seconds(3))
    
    //set checkpoint directory
    ssc.checkpoint("hdfs://192.168.0.111:9000/chkp")
    
    val lines = ssc.socketTextStream("192.168.0.111", 1234, StorageLevel.MEMORY_ONLY)
    
    val words = lines.flatMap(_.split(" "))
    val wordPair = words.map((_,1))
    
    val addFunc = (curreValues:Seq[Int],previousValues:Option[Int]) =>{
      val currentTotal = curreValues.sum
      
      Some(currentTotal + previousValues.getOrElse(0))
    }
    
    val total = wordPair.updateStateByKey(addFunc)
    
    total.print()
    
    ssc.start()
    
    ssc.awaitTermination()
    
  }
}