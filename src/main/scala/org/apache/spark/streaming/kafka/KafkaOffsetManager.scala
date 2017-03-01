package org.apache.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import kafka.common.TopicAndPartition
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.utils.StreamingKafkaUtils

object KafkaOffsetManager {
  
  

  def main(args: Array[String]): Unit = {
     if (args.length < 5) {
      System.err.println("Usage: KafkaWordCount <brokers> <zkQuorum>  <group> <topics> <master>")
      System.exit(1)
    }
  val Array(brokers,zkQuorum, group, topics, master) = args
    val conf = new SparkConf().setMaster(master).setAppName("UserClickCountStat")
   val checkpointPath = "d:/tmp/checkpoint-direct"
 
      val ssc = StreamingContext.getOrCreate(checkpointPath, ()=>{
        val ssc = new StreamingContext(conf, Seconds(5))
    	  ssc.sparkContext.setLogLevel("WARN")
        
    	  val topicsSet =  topics.split(",").toSet
    	  val kafkaParams = Map[String, String](
    			  
    			  "metadata.broker.list" -> brokers,
    			  "serializer.class" -> "kafka.serializer.StringDecoder",
    			  "zookeeper.connect" ->zkQuorum,
    			  "group.id" -> group  
    			  )
            
    //          val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
 //1001
            val messages = StreamingKafkaUtils.createDirectStream(ssc, brokers, topics, kafkaParams)
  
            println("over111")
  messages.foreachRDD((rdd,time)=>{
     rdd.collect().foreach{x=> val str = s"1  __  $time , $x";
     println(str)
     }
  })
  //abc
  //kill
  
  //eeee
   println("over333")
   //更新所有topic的offset
 
	   StreamingKafkaUtils.updateOffset(messages,kafkaParams)//1000 
   
  
        ssc
      })
  
  


  ssc.start
  ssc.awaitTermination()
  }
  
  
  

}