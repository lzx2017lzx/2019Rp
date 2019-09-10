package dayone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object sparkGraph {
  def main(args:Array[String]):Unit = {
    // initiate spark env
    val conf = new SparkConf().setAppName("Spark GraphX Demo").setMaster("local")
    val sc = new SparkContext(conf)
    // define point
    val users = sc.parallelize(Array((3L, ("rxin", "student")),(7L,("jgoke","postman")),(5L,("sam","teacher")),(8L,("tom", "doctor"))))
    
    // define edge
    val relationShip = sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(7L, 8L, "adfew"),
    Edge(8L, 3L, "ewgg"),
    Edge(7L, 5L, "wer")
    ))
    
    // define graph
    val graph = Graph(users, relationShip)    
    
    // use
    val edges_count = graph.edges.filter(e => e.srcId > e.dstId).count()
    
    println("deges_count->" + edges_count)
    
    val prodoc_count = graph.vertices.filter{case(id, (name,pos)) => pos == "ewgg"}.count
    println("prodoc_count-> " + prodoc_count)
    
    sc.stop()
    
  }
}