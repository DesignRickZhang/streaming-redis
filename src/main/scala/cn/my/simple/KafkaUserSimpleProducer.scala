package cn.my.simple
import org.apache.kafka.clients.producer.KafkaProducer

import org.apache.kafka.clients.producer.ProducerRecord
import java.util.HashMap
import org.apache.kafka.clients.producer.ProducerConfig
//params:  spark4:9092 test1 1 1
//spark-submit  --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar --class cn.my.simple.KafkaWordCountProducer  streaming_kafka2.jar dchadoop207:9092 test 1 3 
object KafkaUserSimpleProducer {
  
   def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

//          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//      "kafka.serializer.StringDecoder")
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//      "kafka.serializer.StringDecoder")
      
//      kafka.serializer.StringDecoder
    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    var i =0
   
    while(true) {
   

        val message = new ProducerRecord[String, String](topic, null, i+"")
   
        producer.send(message)
        println(i)
      i = i+1
      
      Thread.sleep(1)
    }
  }
}