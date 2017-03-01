package cn.my.wc

import java.util.HashMap

import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
//cn.my.wc.KafkaWordCount
/***
//[hadoop2@dchadoop207 ~]$ spark-submit  --class cn.my.wc.KafkaWordCount --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar  streaming_kafka.jar dchadoop207 groupSpark test 1 spark://dchadoop207:7077 
*/
 object KafkaWordCount {

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads><master>")
      System.exit(1)
    }

//    StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads,master) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount zsh").setMaster(master)
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L))
//      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(30), Seconds(2), 2)
//    wordCounts.print()
    val words2 = words.map(x=>(x,1L)).reduceByKey(_+_)
    words2.print
//      words2.foreachRDD((rdd,time)=>{
//        val str =   rdd.collect().mkString(",")
//        println(s"----$time:  $str")
//      })
    ssc.start()
    ssc.awaitTermination()
    

  }

}