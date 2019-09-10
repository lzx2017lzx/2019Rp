package daytwo

class GenericClassInt{
//define an inter variable
  private var content:Int = 10

  def set(value:Int) = {
    content  = value
  }

  def get():Int = {content};
}

class GenericClassString{
  private var content:String = ""

  //define get
  def set(value:String) = {content = value}
  def get():String = {content}
}

/**
  * two class are same as with each one
  */


class GenericClass[T] {
private var content:T = _

  def set(value:T)= content = value
  def get():T = content


}

object GenericClass{
def main(args:Array[String]):Unit={
  var vi =new GenericClass[Int]
  vi.set(1000)
  println(vi.get())

  //define a string
  var v2 = new GenericClass[String]
  v2.set("setlzx")
  println(v2.get())
}


}
