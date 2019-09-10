package day728

class Student3(var stuName:String, var age:Int) {
  //define help construct, have a lot of construct

  def this(age:Int){
    this("no name", age)
    println("this is help construct")
  }

}

object Student3{
  def main(args:Array[String]):Unit={
    var s1 = new Student3("Tom", 20)
    println(s1.stuName + "\t" + s1.age)

    var s2 = new Student3(333)
    println(s2.stuName + "\t" + s2.age)

  }


}
