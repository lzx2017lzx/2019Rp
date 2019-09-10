package dayone

import java.io.FileNotFoundException

class DemoException {

}

object ExceptionDemo{
  def main(args:Array[String]):Unit = {
    println("-------------------try catch finally--------------------------")

    try{
      val words = scala.io.Source.fromFile("E:\\scalaPro\\pom.xml").mkString;
    }catch{
      case ex:FileNotFoundException =>{
        println("file not fount")
      }
      case ex:IllegalArgumentException =>{
        println("IllegalArgumentException")
      }
      case _:Exception =>{
        println("other exception")
      }
    }
    finally{
      println("this is finally")
    }
  }
}
