package dayone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager

/*
 * 
 * 
 */


object myTomcatLogToMysql {
def main(args:Array[String]):Unit = {
  val conf = new SparkConf().setMaster("local").setAppName("My Tomcat Log count")
  
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
    
//    rdd1.foreach(f=>{
//      
//      var conn:Connection = null
//    var pst:PreparedStatement = null
//    
//    conn = DriverManager.getConnection("jdbc:mysql://192.168.0.112:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "root", "123")
//    
//    //save data in mysql
//    pst = conn.prepareStatement("insert into mydata values(?,?)")
//    
//      pst.setString(1, f._1)
//      pst.setInt(2, f._2)
//      
//      pst.executeUpdate()
//    })
    
    rdd1.foreachPartition(saveToMysql)
    sc.stop()
}

def saveToMysql(it:Iterator[(String, Int)]) ={
  var conn:Connection = null
    var pst:PreparedStatement = null
//    
    conn = DriverManager.getConnection("jdbc:mysql://192.168.0.112:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "root", "123")
//  
    pst = conn.prepareStatement("insert into mydata values(?,?)")
    
    it.foreach(data =>{
      pst.setString(1, data._1)
      pst.setInt(2,data._2)
      pst.executeUpdate()
    }
      )
    }

}