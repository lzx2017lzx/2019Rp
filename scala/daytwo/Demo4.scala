package daytwo

class Demo4 {

}


class Anima

class Bird extends Anima
class Sparrow extends Bird

class EatSomeThing[-T](t:T)

class demo4 {

}

object demo4{
  def main(args:Array[String]):Unit ={
    var c1:EatSomeThing[Bird] = new EatSomeThing[Bird](new Bird)

    //create a bird eating things
    //var c2:EatSomeThing[Anima] = new EatSomeThing[Anima](new Anima)

    //
    //var c2:EatSomeThing[Anima] = c1

    var c3:EatSomeThing[Sparrow] = c1
  //  var c4:EatSomeThing[Anima] = c3

  }
}


