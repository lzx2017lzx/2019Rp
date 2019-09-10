package dayone

import java.io.FileInputStream

import scala.io.Source._
import java.io.{File, FileInputStream, PrintWriter}
object IODemoone{
  def main(args:Array[String]):Unit={
    //read lines
    val source = fromFile("E:\\scalaPro\\pom.xml")

    println("---------------mkString------------------------")
    //println(source.mkString)
    println("---------------lines------------------------")

    //val lines = source.getLines()
    //lines.foreach(println)

    //read by character
    //for(c <- source)println(c)
    println("-----------------fromURL------------------------")
    //var source2 = fromURL("https://repo1.maven.org/maven2/", "UTF-8")
    //println(source2.mkString)

    println("-----------------readBytes------------------------")
//    var file = new File("E:\\software\\install\\bin\\fsc")
//    //form an inputstream
//    var in = new FileInputStream(file)
//    //construct buffer
//    var buffer = new Array[Byte](file.length().toInt)
//    //read
//    in.read(buffer)
//    println(buffer.length)
//    //close
//    in.close()

    /*
     write file
     */

    var out = new PrintWriter("E:\\scalaPro\\hello.txt")
    for(i <- 0 until 10) out.println(i)
    out.close()
  }
}