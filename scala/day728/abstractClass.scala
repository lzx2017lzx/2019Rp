package day728

abstract class abstractClass {
//define abstract way
  def chechType():String
}


//if subclass have not define the abstract way, subclass is abstract

class Car extends abstractClass{
  override def chechType(): String = "i am a car"
}

class bike extends abstractClass{
  override def chechType(): String = "i am a bike"
}

abstract  class bike2 extends abstractClass{
}
/*
abstract class is only for inherit
 */
object abstractClass{
def main(args:Array[String]):Unit={
  var v1: abstractClass = new Car
  println(v1.chechType())

  var v2: abstractClass = new bike
  println(v2.chechType())
}
}
