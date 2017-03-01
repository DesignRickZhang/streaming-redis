package cn.my.state

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object UpdateStateByKeyTest {
  def main(args: Array[String]): Unit = {
    if(args.length<3){
      println("<hostname><port><master>")
      sys.exit(1)
    }
    val Array(hostname,port,checkPointPath,master) = args

   val sc = StreamingContext.getOrCreate(checkPointPath, ()=>{
     creatContext(hostname, port.toInt, checkPointPath,master)
   })
//    sc.sparkContext.setLogLevel("WARN")
    sc.start()
    sc.awaitTermination()
    
  }
  
  
 def  creatContext(hostname:String,port:Int,checkPointPath :String, master:String): StreamingContext = {
       val conf = new SparkConf().setAppName("UpdateStateByKeyTest").setMaster(master)
    val sc = new StreamingContext(conf,Seconds(5))
//    sc.checkpoint("d:\\tmp\\state_checkpoint2")
       sc.checkpoint(checkPointPath)
//    sc.sparkContext.setLogLevel("WARN")
    val dstream = sc.socketTextStream(hostname, port.toInt)
     def updateFunc(seq:Seq[Int],state:Option[Int]):Option[Int] = {
        println("seq:"+seq+",state:"+state)
      val sum = seq.foldLeft(0)(_+_)
      val result = sum + state.getOrElse(0)
      Option(result)
    }
       
   val state =  dstream.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc)
    state.print
    sc
 }
  
}