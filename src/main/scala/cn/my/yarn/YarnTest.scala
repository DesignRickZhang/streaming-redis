package cn.my.yarn

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import cn.test.yarn.HelloYarn
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

object YarnTest {
  def main(args: Array[String]): Unit = {
    
val pool =    Executors.newCachedThreadPool()
    val conf = new SparkConf().setAppName("YarnTest")
    val sc = new SparkContext(conf)
   val execs = (1 to 3).map(x=>new HelloYarn2("helloyarn_"+x))
   println("execs:"+execs)
   val rdd = sc.parallelize(execs, 3)
  val future = sc.submitJob[HelloYarn2, Unit, Unit](rdd, iter=>{  
     val yarn = iter.next()
     yarn.start()
  },  0 until rdd.partitions.length , (_,_) => Unit, ())
//         val future = ssc.sparkContext.submitJob[Receiver[_], Unit, Unit](
//        receiverRDD, startReceiverFunc, Seq(0), (_, _) => Unit, ())
//      future.onComplete {
//        case Success(_) =>  println()
//        case Failure(e) =>  println()
//    }(pool)
  println("future: "+future)
 while(true){
   Thread.sleep(1000*1000)
 }
println("time to exit")
}
  
}


case class HelloYarn2(name:String){
    def start(){
    startPrintln()
  }
  def startPrintln()={
    var i =0
    while(true){
      println("name:"+name+","+i)
      Thread.sleep(1000)
      i=i+1
    }
  }
}
