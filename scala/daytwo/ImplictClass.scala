package daytwo

class ImplictClass {

}

object ImplictClass{
def main(args:Array[String]):Unit={
  println("two number sum: " + 1.add(2))


  implicit class Calc(x:Int){
    def add(y:Int):Int = x + y
  }
}

}
