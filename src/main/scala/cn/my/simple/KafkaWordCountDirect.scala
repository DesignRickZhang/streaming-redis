package cn.my.simple


import kafka.serializer.StringDecoder
import java.util.HashMap
import kafka.common.TopicAndPartition 
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.utils.StreamingKafkaUtils
import org.apache.spark.Logging
import java.util.concurrent.Executors
//import kafka.common.TopicAndPartition  

//spark4 directGroup direct2 1 local[2]
//cn.my.wc.KafkaWordCount
/***
 * spark4:9092 directCPGroup  topic2  local[6]
 * spark4:9092 g2 topic1 topic2 local[5]
//[hadoop2@dchadoop207 ~]$ spark-submit  --class cn.my.wc.KafkaWordCount --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar  streaming_kafka.jar dchadoop207 groupSpark test 1 spark://dchadoop207:7077 
*/
 object KafkaWordCountDirect extends Logging{
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount33 <brokers>  <group> <topics> <master>")
      println("args:"+args.mkString("\t"),", args.length:"+args.length)
      System.exit(1)
    }
  val Array(brokers,group, topic, master) = args
   println("args:"+args.mkString("\t")+", 223 KafkaWordCountDirect ")
//    StreamingExamples.setStreamingLogLevels()
  
    val kafkaParams = Map[String, String](    
      "metadata.broker.list" -> brokers, 
      "serializer.class" -> "kafka.serializer.StringEncoder",
//      "auto.offset.reset" -> "smallest",
          "auto.offset.reset" -> "largest",
           "group.id"-> group
      )
      
    val conf = new SparkConf().setAppName("KafkaWordCount zsh").setMaster(master)
    println("aaaaaaaaaaaaaaa")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "2")//每秒这么多
//     conf.set("spark.streaming.receiver.maxRate", "3")
     conf.set("spark.streaming.backpressure.enabled","true")
     conf.set("spark.streaming.blockInterval", "5000")
//      conf.set("spark.streaming.kafka.maxRatePerPartition", "2")
//    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.streaming.concurrentJobs","1")
//   val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(3))
        ssc.sparkContext.setLogLevel("WARN")
    val lines = StreamingKafkaUtils.createDirectStream(ssc, brokers, Set(topic), kafkaParams)
//    val executor = Executors.newFixedThreadPool(2)
//    executor.execute(new  Runnable() {
//		def  run() {
////      ssc.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
//			 lines.map{x=> println(111);Thread.sleep(3000);  x}.foreachRDD((rdd,time)=>{
//    rdd.collect().foreach{x=>  
//      val str = s"$time, rdd: $x"
//      println("job1: "+str)  
//    }
//   })
//		}
//	})
//      executor.execute(new  Runnable() {
//    def  run() {
////        ssc.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")
//      lines.map{x=> println(222);Thread.sleep(3000);  x}.foreachRDD((rdd,time)=>{
//      rdd.collect().foreach{x=>  
//        val str = s"$time, rdd: $x"
//        println("job2: "+str)  
//      }
//     })
//    }
//  })
//  StreamingKafkaUtils.updateOffset(lines, kafkaParams)
//   ssc.start()
//    ssc.awaitTermination()
//        lines.foreachRDD(rdd => { 
//          println("getoffset....")  
//          val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//            
//                 for (offsets <- offsetsList) {
//              val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
//              println(topicAndPartition +"," +offsets)
//                 }
//        })
        
        
   lines.map{x=> println(111);Thread.sleep(2000);  x}.foreachRDD((rdd,time)=>{
    rdd.collect().foreach{x=>  
      val str = s"$time, rdd: $x"
//      logInfo("job1: "+str)  
            println("job1: "+str)  
    }
   })
   
   
   
     lines.map{x=> println(222);Thread.sleep(2000);  x}.foreachRDD((rdd,time)=>{
    rdd.collect().foreach{x=>  
      val str = s"$time, rdd: $x"
//      logInfo("job1: "+str)  
            println("job1: "+str)  
    }
   })
   
   
   
//  lines.map{x=> println(222);Thread.sleep(2000);  x}.foreachRDD((rdd,time)=>{
//    rdd.collect().foreach{x=>  
//      val str = s"$time, rdd: $x"
////      logInfo("job2: "+str)  
//            println("job1: "+str)  
//    }
//   })
//   
   

   StreamingKafkaUtils.updateOffset(lines, kafkaParams)
   ssc.start()
    ssc.awaitTermination()
  }

}