package day0617
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
class wordCount{
  
}

object wordCount {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("my scala").setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val result = sc.textFile("hdfs://192.168.0.111:8020/tempfile.txt").flatMap(_.split(" "))
    .map((_,1))
    .reduceByKey(_+_)
    
    
    result.foreach(println)
    
  }
}