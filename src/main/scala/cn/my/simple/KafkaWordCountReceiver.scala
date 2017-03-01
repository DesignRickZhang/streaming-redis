package cn.my.simple


import kafka.serializer.StringDecoder
import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext

//cn.my.wc.KafkaWordCount
/***
//[hadoop2@dchadoop207 ~]$ spark-submit  --class cn.my.wc.KafkaWordCount --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar  streaming_kafka.jar dchadoop207 groupSpark test 1 spark://dchadoop207:7077 
*/
 object KafkaWordCountReceiver {
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
  
  /***
   * 
  kafka + zookeeper,当消息被消费时,
  会想zk提交当前groupId的consumer消费的offset信息,
  当consumer再次启动将会从此offset开始继续消费.
在consumter端配置文件中(或者是ConsumerConfig类参数)有个"autooffset.reset"(在kafka 0.8版本中为auto.offset.reset),有2个合法的值"largest"/"smallest",默认为"largest",此配置参数表示当此groupId下的消费者,在ZK中没有offset值时(比如新的groupId,或者是zk数据被清空),consumer应该从哪个offset开始消费.largest表示接受接收最大的offset(即最新消息),smallest表示最小offset,即从topic的开始位置消费所有消息.
   */
    kafkaParams = Map(
      "zookeeper.connect" ->zkQuorum,
      "group.id" -> group
//      "auto.offset.reset" -> "smallest"
    )
    val conf = new SparkConf().setAppName("KafkaWordCount zsh").setMaster(master)
   val checkpointPath = "d:/tmp/checkpoint-receiver"
//   val ssc = new StreamingContext(conf,Seconds(2))
  val ssc =  StreamingContext.getOrCreate(checkpointPath, ()=>{
	   val ssc = new StreamingContext(conf, Seconds(2))
     ssc.sparkContext.setLogLevel("WARN")
//	    ssc.checkpoint(checkpointPath)
	
	    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
	    val topicsSet = topics.split(",").toSet
	    val lines = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](  ssc, kafkaParams, topicMap, StorageLevel.MEMORY_ONLY)
	    lines.foreachRDD((rdd,time)=>{//rdd: RDD[(String, String)]
	    	val collect = rdd.collect().mkString(",")
	    			val count = rdd.count
	    			println(s"$time, count:  $count , collect:$collect ")
	    })
      ssc
   })
   
    ssc.start()
    ssc.awaitTermination()
  }

}