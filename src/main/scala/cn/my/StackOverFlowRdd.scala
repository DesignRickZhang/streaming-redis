package cn.my

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object StackOverFlowRdd {
  def main(args: Array[String]): Unit = {
    
  def sum(xs:List[Int]) :Int  = xs match{
      case Nil => println("nil");0; 
      case x:: ys =>  
//        println("x: "+x+", ys:"+ ys); 
        x + sum(ys)
    }
  
  val conf = new SparkConf().setAppName("StackOverFlowRdd").setMaster("local");
  val sc = new SparkContext(conf)
//  sc.parallelize(1 to 3).map { x =>  sum((x to 10000).toList) } //把在scala中的代码放到executor上执行
//  .count() 
  var rdd = sc.parallelize(1 to 3)
  for(i <- 1 to 10000){
   rdd = rdd.map(x=>x)
  }
  rdd.count
  
  }
}