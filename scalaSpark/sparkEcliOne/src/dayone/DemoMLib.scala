package dayone

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object DemoMLib {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder().appName("LinearRegration").master("local").getOrCreate()
    
    //read data
    val data_path = "E:\\sample_linear_regression_data.txt"
    
    val lr = new LinearRegression().setMaxIter(10000)
    
    val trainning = spark.read.format("libsvm").load(data_path)
    
    val lrModel = lr.fit(trainning)
      
    val trainningSummary = lrModel.summary
    trainningSummary.predictions.show()
    
    println(s"RMSE:${trainningSummary.rootMeanSquaredError}")
    
    spark.stop()
    
    
    
    
  }
}