package daytwo


class Vehicle{

}

case class Car(name:String) extends Vehicle{

}

case class Bike(name:String) extends Vehicle{

}



object Demo2 {
  def main(args: Array[String]): Unit = {
  var aCar:Vehicle = new Car("car")

  aCar match{
  case Car(name) =>println("auto car" + name)
  case Bike(name) =>println("bike" + name)
  case _ => println("others")
}

}
}