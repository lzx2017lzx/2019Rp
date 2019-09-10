package day728

/*
apply way

 */
class Student4(var stuName:String)

//define object

object Student4{

  def apply(name:String) = {
    println("use apply way")
    new Student4(name)
  }

  def main(args:Array[String]):Unit = {
    //by main construct to create student object
    var s1 = new Student4("lzx")
    println(s1.stuName)

    var s2 = Student4("llslsls")
    println(s2.stuName)
  }
}
