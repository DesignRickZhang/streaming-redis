package cn.my.wang

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object ReduceByKeyAndWindow {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("app")
    val ssc = new StreamingContext(conf,Seconds(5))
    val dstream =  ssc.socketTextStream("spark4", 9999, StorageLevel.MEMORY_ONLY)
    import StreamingContext._
   val hostestDstream = dstream.map(_.split(" ")).map { x => (x(0),1) }.reduceByKeyAndWindow((v1:Int,v2:Int)=>v1+v2, Seconds(60) ,Seconds(20))
  hostestDstream.transform(hostestItemRDD =>{
   val top3 =  hostestItemRDD.map(pair => (pair._2,pair._1)).sortByKey(false).map(pair=>(pair._2,pair._1)).take(3)
   top3.foreach(x=>println("top3..."+x))
    hostestItemRDD
  }).print
   
   ssc.start
    ssc.awaitTermination()
  }

}