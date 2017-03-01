package cn.my.yarn
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import cn.test.yarn.HelloYarn
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

object YarnTest2 {
  def main(args: Array[String]): Unit = {
    
val pool =    Executors.newCachedThreadPool()
    val conf = new SparkConf().setAppName("YarnTest2")
    val sc = new SparkContext(conf)
   val execs = (1 to 3).map(x=>new HelloYarn2("helloyarn_"+x))
   println("execs:"+execs)
   val rdd = sc.parallelize(execs, 3)
   
   rdd.map {x=>x.start }.count
   rdd.collect
   while(true){
   Thread.sleep(1000*1000)
 }
println("time to exit")
}
}