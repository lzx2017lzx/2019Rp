package dayone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import java.sql.DriverManager

object antherJdbcDemo {
  val connection = () =>{
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    DriverManager.getConnection("jdbc:mysql://192.168.0.112:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "root", "123")
  }
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("my jdbc rdd demo").setMaster("local")
    
    val sc = new SparkContext(conf)
    
    var str = new String("head.jsp")
    
    val mysqlRDD = new JdbcRDD(sc,connection, "select * from mydata where countNumber = ? and countNumber = ?", 1, 1, 2, r=>{
      
      //handle result returned
      val ename = r.getString(1)
      val countNumber = r.getInt(2)
      (ename, countNumber)
    })
    
    val result = mysqlRDD.collect()
    println(result.toBuffer)
    sc.stop()
    }
}