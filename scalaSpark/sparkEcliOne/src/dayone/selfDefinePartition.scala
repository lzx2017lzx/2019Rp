package dayone

import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap

class selfDefinePartition {
  
}

object selfDefinePartition {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setMaster("local").setAppName("My Tomcat Log Parition")
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.textFile("E:\\sparkTest\\localhost_access_log.txt")
          .map(
              line => {
      //resolve string
      val index1 = line.indexOf("\"")
      val index2 = line.lastIndexOf("\"")
      val line1 = line.substring(index1+1, index2)
      
      
      val index3 = line1.indexOf(" ")
      val index4 = line1.lastIndexOf(" ")
      val line2 = line1.substring(index3+1, index4)
      
      val jspName = line2.substring(line2.lastIndexOf("/")+1)
      
      (jspName, line)
    }
              
              )
              
              //self define partition rule
       val rdd2 = rdd1.map(_._1).distinct().collect()
       
       //
       val myParititioner = new MyWebParitioner(rdd2)
    
       val rdd3 = rdd1.partitionBy(myParititioner)
       
       //out
       rdd3.saveAsTextFile("E:\\sparkTest\\temp\\test_partition")
       
       sc.stop()
  }
}

class MyWebParitioner(jspList:Array[String])extends Partitioner{
  //
  val partitionMap = new HashMap[String, Int]()
  
  var partId = 0
  
  for(jsp <- jspList){
    partitionMap.put(jsp, partId)
    partId += 1
  }
  
  def numPartitions:Int = partitionMap.size
  
  def getPartition(key:Any):Int = partitionMap.getOrElse(key.toString(), 0)
  
  
  
}