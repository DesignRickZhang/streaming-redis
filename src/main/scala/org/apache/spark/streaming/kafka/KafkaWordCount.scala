package org.apache.spark.streaming.kafka

import kafka.serializer.StringDecoder

import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

//cn.my.wc.KafkaWordCount
/***
//[hadoop2@dchadoop207 ~]$ spark-submit  --class cn.my.wc.KafkaWordCount --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar  streaming_kafka.jar dchadoop207 groupSpark test 1 spark://dchadoop207:7077 
*/
 object KafkaWordCount {
  private var kafkaParams: Map[String, String] = _
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads><master>")
      System.exit(1)
    }
  val Array(zkQuorum, group, topics, numThreads,master) = args
//    StreamingExamples.setStreamingLogLevels()
//    val Array(zkQuorum, group, topics, numThreads,master) = Array("dchadoop207","groupSpark","test", "1")
// val zkQuorum = "dchadoop207"
//  val zkQuorum = "spark4"
// val group = "groupSpark"
// val topics = "test2"
// val numThreads = "1"
  
    kafkaParams = Map(
      "zookeeper.connect" ->zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )
    //        val Array(zkQuorum, group, topics, numThreads,master) = args
    val conf = new SparkConf().setAppName("KafkaWordCount zsh").setMaster(master)
   val checkpointPath = "d:/tmp/checkpoint-direct"
   val ssc = new StreamingContext(conf,Seconds(2))
//  val ssc =  StreamingContext.getOrCreate(checkpointPath, ()=>{
//     val ssc2 = new StreamingContext(conf, Seconds(2))
//     ssc2.sparkContext.setLogLevel("WARN")
//      ssc2.checkpoint(checkpointPath)
//      ssc2
//   })

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap,StorageLevel.MEMORY_ONLY).map(_._2)
//     val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

//     val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
////
//        val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, topicMap, StorageLevel.MEMORY_ONLY)
  lines.print
   
    ssc.start()
    ssc.awaitTermination()
  }

}