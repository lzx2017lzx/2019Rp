package day728

import scala.collection.mutable.ArrayBuffer

class InternalClass {
//define student property
  private var stuName:String = "lzx"
  private var stuAge:Int = 20
  private var courseList = new ArrayBuffer[Course]()

  def addNewCourse(cname:String, grade:Int)={
    var c = new Course(cname, grade)

    courseList += c

  }


  //define course class


  class Course(var courseName:String, var grade:Int){

  }
}

object InternalClass{
  def main(args:Array[String]):Unit={
    var s = new InternalClass

    s.addNewCourse("Chinese", 80)
    s.addNewCourse("Math", 70)
    s.addNewCourse("English", 90)

    //sout
    println(s.stuName + "\t" + s.stuAge)

    println("----------course information------------")
    for(c <- s.courseList)println(c.courseName + "\t" + c.grade)
  }


}