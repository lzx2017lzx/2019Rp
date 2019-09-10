package daytwo

class ImplicitDemo {

}

class Fruit(name:String){
  def getFruitName():String = name
}

class Monkey(f:Fruit){
  def say() = println("Monke like") + f.getFruitName()
}

object ImplicitDemo{
def main(args:Array[String]):Unit ={
  var f:Fruit = new Fruit("banana")

  f.say()

}

  //define implicit transfer function
  implicit def fruit2Monkey(f:Fruit):Monkey = {
    new Monkey(f)
  }

  // implicit advice used seriously

}
