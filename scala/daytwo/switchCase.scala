package daytwo

object switchCase {

  def main(args:Array[String]):Unit={
    var chi = '='
    var sign = 0

    chi match{
      case '+' => sign = 1
      case '-' => sign = -1
      case _   => sign = 0
    }

    println(sign)

    /*


     */
  var ch2 = '6'
    var digit: Int = -1

    ch2 match{
      case '+' =>println("this is plus num")
      case '-' =>println("this is a minus")
      case _ if Character.isDigit(ch2) =>digit = Character.digit(ch2, 10)
      case _ => println("this is others")
    }

    println(digit)
  }

  /**
    * 3. in regular match claim variable
    */

  var mystr = "Hello World"
  mystr(7) match {
    case '+' => println("this is a plus")
    case '-' =>println("this is a minus")
    case ch =>println(ch)
  }

  /**
    * 4.match type, instanceof
    * use: case
    */


  var v4:Any = 100
  v4 match{
    case x:Int => println("this is integer")
    case s:String =>println("this is a string")
    case _ => println("other type")
  }

  /**
    * 5 mtch array and list
    */

  var myArray = Array(1, 3, 3)
  myArray match{
    case Array(0) => println("there is only one zero in array")
    case Array(x, y) => println("there are two elements in array")
    case Array(x, y, z) => println("there are three elements in array")
    case Array(x, _*) => println("there are many elements")
  }

  var myList = List(1, 2,3)
  myList match{
    case List(0) => println("there is only one zero in List")
    case List(x, y) => println("there are two elements in List, sum is: " + (x+y))
    case List(x, y, z) => println("there are three elements in List" + (x+y+z))
    case List(x, _*) => println("there are many elements" + myList.sum)
  }
}
