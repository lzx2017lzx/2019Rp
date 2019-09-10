//package day729
//
//import akka.actor.{Actor, ActorSystem, Props}
//
//class demo1 extends Actor{
//  override def receive: Receive = {
//    case "Hello" => println("ni hao")
//    case _ => println("you ar ?")
//  }
//}
//
//object demo1{
//def main(args:Array[String]):Unit ={
//  val system = ActorSystem("HelloSystem")
//
//  val helloActor = system.actorOf(Props[demo1],name="helloactor")
//
//  helloActor ! "Hello"
//
//  }
//}