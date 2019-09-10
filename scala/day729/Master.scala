//package day729
//
//import akka.actor._
//import com.typesafe.config.ConfigFactory
//import scala.concurrent.duration._
//import scala.collection.mutable
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.collection.mutable
//import scala.language.postfixOps
//
//
//class Master extends Actor{
//  val idToWorker = new mutable.HashMap[String, WorkerInfo]()
//
//  val workers = new mutable.HashSet[WorkerInfo]()
//
//  val WORKER_TIMEOUT = 10 * 1000
//
//  override def receive:Receive= {
//    case RegisterWorker(id, workerHost, memory, cores) => {
//      if (!idToWorker.contains(id)) {
//        val worker = new WorkerInfo(id, workerHost, memory, cores)
//        workers.add(worker)
//        idToWorker(id) = worker
//        println("new register worker: " + worker)
//        sender ! RegisteredWorker(worker.id)
//      }
//    }
//
//    case SendHeartBeat(workerId) => {
//      val workerInfo = idToWorker(workerId)
//      println("get heartbeat message from " + workerInfo)
//      workerInfo.lastHeartBeat = System.currentTimeMillis()
//    }
//
//    case CheckOfTimeOutWorker => {
//      val currentTime = System.currentTimeMillis()
//
//      val toRemove = workers.filter(w => currentTime - w.lastHeartBeat > WORKER_TIMEOUT).toArray
//
//      for (worker <- toRemove) {
//        workers -= worker
//        idToWorker.remove(worker.id)
//      }
//
//      println("worker size: " + workers.size)
//    }
//  }
//
//      override def preStart():Unit ={
//      context.system.scheduler.schedule(5 millis, WORKER_TIMEOUT millis,self, CheckOfTimeOutWorker)}
//
//}
//
//object Master{
//  def main(args: Array[String]):Unit = {
//    val host = "localhost"
//    val port = 8888
//
//    val configStr =
//      s"""
//         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
//         |akka.remote.netty.tcp.hostname = "$host"
//         |akka.remote.netty.tcp.port = "$port"
//       """.stripMargin
//
//    val config = ConfigFactory.parseString(configStr)
//    val actorSystem = ActorSystem.create("MasterActorSystem", config)
//
//    actorSystem.actorOf(Props[Master], "Master")
//  }
//}