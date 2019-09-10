package scalaObject


case class Person(name:String, age:Int){
  override def toString: String ={
    "name:" + name + ", age: " + age
  }
}


class demo1 {

}

object demo1{
  def main(args: Array[String]):Unit = {

    implicit object PersonOrdering extends Ordering[Person]{
      override def compare(x:Person, y:Person):Int = {
        x.name == y.name match{
          case false => x.name.compareTo(y.name)
          case _ => x.age - y.age
        }
      }
    }
    val p1 = new Person("rain", 13)
    val p2 = new Person("rain", 14)


    import Ordered._
    println(p1 < p2)
  }
}
