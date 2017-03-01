package org.apache.spark.streaming.kafka.utils

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.streaming.kafka.HasOffsetRanges
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

object StreamingKafkaUtils {
  
  
  def createDirectStream(ssc: StreamingContext, brokers:String, topicsSet:Set[String], kafkaParams : Map[String, String]): InputDStream[(String, String)] = {


    //    val kafkaParams = Map[String, String](
    //      "metadata.broker.list" -> brokers,
    //      "serializer.class" -> "kafka.serializer.StringDecoder",
    ////      "zookeeper.connect" ->zkQuorum,
    //      "group.id" -> group,
    //      //    "auto.offset.reset" -> "smallest"
    //      "auto.offset.reset" -> "largest"
    //    )
    val group = kafkaParams.get("group.id").get
    val kc = new KafkaCluster(kafkaParams)
    //从zookeeper上读取offset开始消费message
    val messages = {
      val kafkaPartitionsE = kc.getPartitions(topicsSet)
      if (kafkaPartitionsE.isLeft) throw new SparkException("get kafka partition failed:")
      val kafkaPartitions = kafkaPartitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(group, kafkaPartitions)
      if (consumerOffsetsE.isLeft) {
        println("isLeft...")
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      println(messages)
        messages
        //    throw new SparkException("get kafka consumer offsets failed:"+consumerOffsetsE.left.get)

      }else{
        println("isRight...")
        val consumerOffsets = consumerOffsetsE.right.get
        consumerOffsets.foreach {
          case (tp, n) => println("===================================" + tp.topic + "," + tp.partition + "," + n)
        }
        val leaderEalisestOffset = kc.getEarliestLeaderOffsets(kafkaPartitions).right.get
        val flag = consumerOffsets.forall{
          case (tp,n)=> n< leaderEalisestOffset(tp).offset
        }
        if(flag){
          println("consumer group:"+group +" offset已经过时，更新为leaderEarliestOffset")
          val offsets = leaderEalisestOffset.map{
            case (tp,offset)=> (tp,offset.offset)
          }
          kc.setConsumerOffsets(group, offsets)
        }else{
          println("consumer group: "+group+" offsets正常, 无需更新")
        }
        
        
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
          ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
      }
    }
    messages
  }
  def createDirectStream(ssc: StreamingContext, brokers:String, topics:String, kafkaParams : Map[String, String]) : InputDStream[(String, String)] = {
    val topicsSet =  topics.split(",").toSet
    createDirectStream(ssc,brokers,topicsSet,kafkaParams)
  }
  
//    def updateOffset[T](dstream: DStream[T],kafkaParams:Map[String, String]): Unit = {
//    updateOffset(dstream,kafkaParams) 
//  }
    
    
  def updateOffset[T](dstream: DStream[T],kafkaParams:Map[String, String]) ={
          dstream.foreachRDD(rdd => { 
            val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            val kc = new KafkaCluster(kafkaParams)
            for (offsets <- offsetsList) {
              val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
//              if(offsets.fromOffset==offsets.untilOffset)
              val group = kafkaParams.get("group.id").get
              println(s"topicAndPartition: $topicAndPartition , offsets: $offsets " +",offsets.partition:"+ offsets.partition +  " , offsets.untilOffset: "+ offsets.fromOffset +" , offsets.untilOffset: "+ offsets.untilOffset  )
               val o = kc.setConsumerOffsets(group, Map((topicAndPartition, offsets.untilOffset)))
              if (o.isLeft) {
                println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
              }else{
                println("update Now")
              }
            }
      })
  }
      
}