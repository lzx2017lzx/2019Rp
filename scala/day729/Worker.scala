//package day729
//
//import java.util.UUID
//
//import akka.actor.{Actor, ActorSelection}
//import scala.language.postfixOps
//import java.util.UUID
//import scala.concurrent.duration._
//import scala.concurrent.ExecutionContext.Implicits.global
//import akka.actor._
//import com.typesafe.config.ConfigFactory
//
//class Worker extends Actor{
//  var master: ActorSelection = null
//
//  val id = UUID.randomUUID().toString
//
//  override def preStart(): Unit = {
//    master = context.system.actorSelection("akka.tcp://MasterActorSystem@localhost:8888/user/Master")
//
//    master ! RegisterWorker(id, "localhost", "10240", "8")
//
//  }
//
//  override def receive: Receive = {
//    case RegisteredWorker(masterUrl)=>{
//      context.system.scheduler.schedule(0 millis, 500 millis, self, HeartBeat)
//    }
//
//    case HeartBeat => {
//      println("sent heartbeat")
//      master ! SendHeartBeat(id)
//    }
//  }
//
//}
//
//object Worker{
//  def main(args: Array[String]):Unit = {
//    val clientPort = 8892
//
//    val configStr =
//      s"""
//         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
//         |akka.remote.netty.tcp.port = $clientPort
//       """.stripMargin
//
//    val config = ConfigFactory.parseString(configStr)
//
//    val actorSystem = ActorSystem("WorkerActorSystem", config)
//
//    actorSystem.actorOf(Props[Worker], "Worker")
//  }
//}
