package dayone

import org.apache.spark.sql.SparkSession

/*
 * use caseclass create dataframe
 */
object Demo2 {
  def main(args: Array[String]):Unit={
    val spark = SparkSession.builder().master("local").appName("Demo2").getOrCreate()
    
    val lineRDD = spark.sparkContext.textFile("E:\\sparkTest\\student.txt").map(_.split("\t"))
    
    //
    val studentRDD = lineRDD.map(x=>Student(x(0).toInt,x(1),x(2).toInt))
    
    import spark.sqlContext.implicits._
    val studentDF = studentRDD.toDF
    
    studentDF.createOrReplaceTempView("student")
    
    spark.sql("select * from student").show
    
    spark.stop()
  }
}

//define case class
case class Student(stuId:Int,stuName:String, stuAge:Int)