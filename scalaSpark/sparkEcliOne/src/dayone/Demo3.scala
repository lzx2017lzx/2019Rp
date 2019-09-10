package dayone


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import java.util.Properties

/*
 * write result into mysql
 */
object Demo3 {
def main(args:Array[String]):Unit={
  //create spark session object
    val spark = SparkSession.builder().master("local").appName("Demo1").getOrCreate()
    
    val personRDD = spark.sparkContext.textFile("E:\\sparkTest\\student.txt").map(_.split("\t"))
    
    val schema = StructType(
        List(
        StructField("stuId", IntegerType),
        StructField("stuName", StringType),
        StructField("stuAge", IntegerType)
        )
        )
        
    //RDD transform rowRDD
       val rowRDD = personRDD.map(p => Row(p(0).toInt,p(1).trim(),p(2).toInt))
           
       val personDataFrame = spark.createDataFrame(rowRDD, schema)
       
       personDataFrame.createOrReplaceTempView("t_person")
       
       val result = spark.sql("select * from t_person order by stuAge desc")
       
       //df.show()
       val props = new Properties()
    
       props.setProperty("user", "root")
       props.setProperty("password", "123")
       
       result.write.mode("append").jdbc("jdbc:mysql://192.168.0.112:3306/company?serverTimezone=UTC&characterEncoding=utf-8","student",props)
       
       
       
       
       
       spark.stop()
}
}

