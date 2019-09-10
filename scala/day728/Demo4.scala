package day728

//scala property
class Demo4 {

}

//define two traits

//define father class
trait Human{
  val id:Int
  val name:String
}

trait  Action{
  //define abstract function
  def getActionName():String
}

//define sub class inherited from
class Student5(val id:Int, val name:String) extends Human with Action{
  override def getActionName(): String = "Action is coming"
}

object Demo4{
  def main(args:Array[String]):Unit = {
    val s1 = new Student5(1, "tom")
    println(s1.id + "\t" + s1.name)
    println(s1.getActionName())
  }
}


