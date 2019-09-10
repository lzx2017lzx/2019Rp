package scalaObject

class upperAnd {

}

class Father
class Son extends Father
class Son1 extends  Son
class Son2 extends Son1


object Demo2{
  def main(args: Array[String]):Unit = {
    //fun2(new Son2)
    var father:Father = new Son2
    fun2(father)
  }

def fun[T<:Son](x:T)={
  println("123")
}

  def fun2[T>:Son](x:T):Unit = {
    println("456")
  }
}