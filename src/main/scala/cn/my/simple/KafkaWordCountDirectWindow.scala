package cn.my.simple


import kafka.serializer.StringDecoder
import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.utils.StreamingKafkaUtils

//spark4 directGroup direct2 1 local[2]
//cn.my.wc.KafkaWordCount
/***
 * 
 * spark4:9092 g2 test1 local[2]
//[hadoop2@dchadoop207 ~]$ spark-submit  --class cn.my.wc.KafkaWordCount --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar  streaming_kafka.jar dchadoop207 groupSpark test 1 spark://dchadoop207:7077 
*/
 object KafkaWordCountDirectWindow {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: KafkaWordCount <brokers>  <group> <topics> <master>")
      println("args:"+args.mkString("\t"))
      System.exit(1)
    }
  val Array(brokers,group, topics, master) = args
   println("args:"+args.mkString("\t"))
//    StreamingExamples.setStreamingLogLevels()
//    val Array(zkQuorum, group, topics, numThreads,master) = Array("dchadoop207","groupSpark","test", "1")
// val zkQuorum = "dchadoop207"
//  val zkQuorum = "spark4"
// val group = "groupSpark"
// val topics = "test2"
// val numThreads = "1"
  
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, 
      "serializer.class" -> "kafka.serializer.StringEncoder",
//      "auto.offset.reset" -> "smallest",
          "auto.offset.reset" -> "largest",
           "group.id"-> group
      )
      
    val conf = new SparkConf().setAppName("KafkaWordCount zsh").setMaster(master)
    conf.set("spark.streaming.kafka.maxRatePerPartition", "2")
    
//   val ssc = new StreamingContext(conf,Seconds(3))
//     val ssc = new StreamingContext(conf,Seconds(3))
//  1461652707000 ms, rdd: (null,540)
//1461652707000 ms, rdd: (null,541)
//1461652707000 ms, rdd: (null,542)
//16/04/26 14:38:30 WARN VerifiableProperties: Property serializer.class is not valid
//1461652710000 ms, rdd: (null,543)
//1461652710000 ms, rdd: (null,544)
//1461652710000 ms, rdd: (null,545)
//  val ssc =  StreamingContext.getOrCreate(checkpointPath, ()=>{
	   val ssc = new StreamingContext(conf, Seconds(3))
        ssc.sparkContext.setLogLevel("WARN")
      
        //  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val topicsSet = topics.split(",").toSet
//   val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      //  val lines = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](  ssc, kafkaParams, topicMap, StorageLevel.MEMORY_ONLY)
 val lines = StreamingKafkaUtils.createDirectStream(ssc, brokers, topics, kafkaParams)
   lines.map{x=> println(111);Thread.sleep(3000);  x}.foreachRDD((rdd,time)=>{
    rdd.collect().foreach{x=>  
      val str = s"$time, rdd: $x"
//      Thread.sleep(5000)
      println("job1: "+str)  
    }
//  })
   })
//  lines.map{x=> println(222);Thread.sleep(3000);  x}.foreachRDD((rdd,time)=>{
//    rdd.collect().foreach{x=>  
//      val str = s"$time, rdd: $x"
//
//      println("job2: "+str)  
//    }
////  })
//   })
//      .groupByKey()
//                                   .count()
   lines.map(_._2).flatMap(_.split(",")(0)).map((_,1)).window(Seconds(5), Seconds(5)).reduceByKey(_+_)
   

   StreamingKafkaUtils.updateOffset(lines, kafkaParams)
    ssc.start()
    ssc.awaitTermination()
  }

}