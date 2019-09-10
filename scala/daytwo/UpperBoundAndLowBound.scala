//package daytwo
//
//class UpperBoundAndLowBound {
//
//}
//
////define father class
////class Vehicle {
////  def driver():Unit= println("drivering")
////}
////
//////define two subclass
////class Car extends Vehicle{
////  override def driver():Unit = println("Car Driver")
////}
////
////class Bike extends  Vehicle{
////  override def driver():Unit = println("Bike Driver")
////}
////
////object ScalaUpperBount{
////
////  def takeVehicle[T <: Vehicle](v:T) = v.driver
////  def main(args:Array[String]):Unit={
////  //define vehicle
////    var v : Vehicle = new Vehicle
////    takeVehicle(v)
////
////    var c:Car = new Car
////    takeVehicle(c)
////}
////}
//
//
//class Vehicle{
//  def drive() = println("Driving")
//}
//
////定义两个子类
//class Car extends Vehicle{
//  override def drive(): Unit = println("Car Driving")
//}
//
//class Bike extends Vehicle{
//  override def drive(): Unit = println("Bike Driving")
//}
//
//object ScalaUpperBount {
//
//  //定义驾驶交通工具的函数，规定上界是Vehicle
//  def takeVehicle[T <: Vehicle](v:T):Unit = v.drive
//
//  def main(args: Array[String]): Unit = {
//    //定义交通工具
//    var v : Vehicle = new Vehicle
//    takeVehicle(v)
//
//    var c : Car  = new Car
//    takeVehicle(c)
//
//    //takeVehicle("sdkfjlsjflksd")
//    /**
//      * 注意：不能传递其他类型，例如takeVehicle("sdkfjlsjflksd")
//      *
//      * 因为String不是V的子类
//      */
//
//
//
//  }
//
//}