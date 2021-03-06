package cn.my.state


import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming._
object ReliableStatefullNetworkWordCount {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: StatefulNetworkWordCount <hostname> <port> <checkPointPath><master>")
      System.exit(1)
    }
    val Array(ip,port,checkPointPath,master) = args


    val ssc = StreamingContext.getOrCreate(checkPointPath, ()=>{
      creatContext(args(0),args(1).toInt,checkPointPath,master)}
    )
    ssc.sparkContext.setLogLevel("WARN")
    ssc.start()
    ssc.awaitTermination()
  }
  
 def  creatContext(ip:String,port:Int,checkPointPath :String,master:String): StreamingContext = {
     val conf = new SparkConf().setAppName("StatefulNetworkWordCount").setMaster(master)
    // Create the context with a 1 second batch size
   val ssc = new StreamingContext(conf,Seconds(5))
//     ssc.sparkContext.setLogLevel("WARN")
//    ssc.checkpoint(checkPointPath)  //在这里checkpoint，可能会造成state的结果偏少， 因为会漏掉后面计算完成任务，但没有checkpoint的这部分

     // Initial state RDD for mapWithState operation
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

    // Create a ReceiverInputDStream on target ip:port and count the
    // words in input stream of \n delimited test (eg. generated by 'nc')
    val lines = ssc.socketTextStream(ip,port.toInt)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    // Update the cumulative count using mapWithState
    // This will give a DStream made of state (which is the cumulative count of the words)
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }
    
    

    val stateDstream = wordDstream.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))
    stateDstream.print()
     ssc.checkpoint(checkPointPath)
    ssc
 }
}
// scalastyle:on println
