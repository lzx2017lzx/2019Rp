package day728

//define class
class Studentone {

  //define property
  private var stuId:Int = 0
  private var stuName:String = "Tom"
  private var age:Int = 20

  //define member way
  def getStuName():String = stuName
  def setStuName(newName:String) = this.stuName = newName

  def getStuAge():Int = age
  def setStuAge(newAge:Int) = this.age = newAge

}

/*
test studentone
 */

object Studentone{
  def main(args:Array[String]):Unit ={
    //create a student object
  var s1 = new Studentone

    //visit property and system out
    println(s1.getStuName() + "\t" + s1.getStuAge())


    s1.setStuAge(30)
    s1.setStuName("lzx")
    print(s1.getStuName() + "\t" + s1.getStuAge())

    /*
    director visit private
     */
    println("------------------visist property directly-----------------")
    println(s1.stuId + "\t" + s1.stuName)

    /*
    visit object private member
     */


  }
}
