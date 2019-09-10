package dayone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class clusterwordCount {
  
}

object clusterwordCount{
  def main(args:Array[String]):Unit = {
   println("--------mkString---------")
   println("--------mkString---------")
    val conf = new SparkConf().setAppName("my scala cluster word count")
    
    val sc = new SparkContext(conf)
    
//    val result = sc.textFile("hdfs://192.168.0.111:9000/tempfile.txt").flatMap(_.split(" "))
//    .map((_,1))
//    .reduceByKey(_+_)
//    
//    
//    result.foreach(println)
   
       val result = sc.textFile(args(0)).flatMap(_.split(" "))
    .map((_,1))
    .reduceByKey(_+_)
    .saveAsTextFile(args(1))
    
    
   // result.foreach(println)
    sc.stop()
  }
}