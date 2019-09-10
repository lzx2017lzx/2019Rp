package day728

/*
scala extends

 */

class Person(val name:String, val age:Int){
  //define functon

  def sayHello():String = "Hello" + name + "and the age is:" + age
}

//define sub class
class Employee(override val name:String, override val age:Int, val salary:Int) extends  Person(name, age){
  override def sayHello(): String = "say hello in sub class"
}

object Demo1 {
  def main(args:Array[String]):Unit = {
    //create person object
    var p1 = new Person("Tom", 20)
    println(p1.name + "\t" + p1.age)
    println(p1.sayHello())

    //create employee object
    var p2:Person = new Employee("Mike", 22, 3000)
    println(p2.sayHello())

    //by annonmy to make inherit
    var p3:Person = new Person("lzx", 22){
      override def sayHello(): String = "annonmy say hello"
    }

    println(p3.sayHello())
  }
}
