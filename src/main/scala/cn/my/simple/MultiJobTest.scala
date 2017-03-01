package cn.my.simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//多线程提交job
object MultiJobTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[8]")
    conf.setAppName("app")
    val sc = new SparkContext(conf)
//   val rdd3 = sc.parallelize(1 to 100, 3).map { x => Thread.sleep(1000);println(x); x }
//   rdd3.count
    new Thread(new  Runnable() {
    	def  run() {
        println("run11.....")
          val rdd1 = sc.parallelize(1 to 1000, 3).map { x => Thread.sleep(1000);println(s"thread1: $x"); x }
 
        println("rdd1.run")
    		rdd1.count
    	}
    }).start
     new Thread(new  Runnable() {
      def  run() {
           val rdd2 = sc.parallelize(1 to 1000, 3).map { x => Thread.sleep(1000);println("thread2: "+x); x }
             println("rdd1.run")
        rdd2.count
      }
    }).start
     while(true){
    Thread.sleep(1000)
    println(println("sleep"))    
  }
  }
 
  
}