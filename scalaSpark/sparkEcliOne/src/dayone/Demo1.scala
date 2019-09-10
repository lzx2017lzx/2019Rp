package dayone

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row

/*
 * create dataframe exe sql
 */
object Demo1 {
  def main(args:Array[String]):Unit = {
    //create spark session object
    val spark = SparkSession.builder().master("local").appName("Demo1").getOrCreate()
    
    val personRDD = spark.sparkContext.textFile("E:\\sparkTest\\student.txt").map(_.split("\t"))
    
    val schema = StructType(
        List(
        StructField("Id", IntegerType),
        StructField("name", StringType),
        StructField("age", IntegerType)
        )
        )
        
    //RDD transform rowRDD
       val rowRDD = personRDD.map(p => Row(p(0).toInt,p(1).trim(),p(2).toInt))
           
       val personDataFrame = spark.createDataFrame(rowRDD, schema)
       
       personDataFrame.createOrReplaceTempView("t_person")
       
       val df = spark.sql("select * from t_person order by age desc")
       
       df.show()
       
       spark.stop()
           
  }
}