package dayone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext

class MyTomcatLog {
  
}

object MyTomcatLog{
  def main(args: Array[String]):Unit={
    val conf = new SparkConf().setMaster("local").setAppName("my tomcat log count")
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.textFile("E:\\sparkTest\\localhost_access_log.txt")
    .map(line => {
      //resolve string
      val index1 = line.indexOf("\"")
      val index2 = line.lastIndexOf("\"")
      val line1 = line.substring(index1+1, index2)
      
      
      val index3 = line1.indexOf(" ")
      val index4 = line1.lastIndexOf(" ")
      val line2 = line1.substring(index3+1, index4)
      
      val jspName = line2.substring(line2.lastIndexOf("/")+1)
      
      (jspName, 1)
    })
    
    //
    val rdd2 = rdd1.reduceByKey(_+_)
    
    val rdd3 = rdd2.sortBy(_._2,false)
    
    //get the highest website
    rdd3.take(2).foreach(println)
    
    sc.stop()
  }
}