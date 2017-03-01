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
import java.util.concurrent.Executors

//spark4 directGroup direct2 1 local[2]
//spark4:9092 directCPGroup topic1 topic2  local[6]
//cn.my.wc.KafkaWordCount
/***
 * 
 * spark4:9092 g2 test1 test2 local[2]
//[hadoop2@dchadoop207 ~]$ spark-submit  --class cn.my.wc.KafkaWordCount --jars spark-streaming-kafka-assembly_2.10-1.6.0.jar  streaming_kafka.jar dchadoop207 groupSpark test 1 spark://dchadoop207:7077 
*/
 object KafkaWordCountDirect_twoStream {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaWordCountDirect_twoStream <brokers>  <group> <topics> <master>")
      println("args:"+args.mkString("\t"))
      System.exit(1)
    }
  val Array(brokers,group, topic1, topic2, master) = args
   println("args:"+args.mkString("\t")+", KafkaWordCountDirect_twoStream...")
//    StreamingExamples.setStreamingLogLevels()
  
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, 
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "auto.offset.reset" -> "smallest",
//          "auto.offset.reset" -> "largest",
           "group.id"-> group
      )
      
    val conf = new SparkConf().setAppName("KafkaWordCount zsh").setMaster(master)
    conf.set("spark.streaming.kafka.maxRatePerPartition", "2")
//   conf.set("spark.scheduler.mode", "FAIR")  
     conf.set("spark.streaming.concurrentJobs","2")
//spark.scheduler.mode
     val ssc = new StreamingContext(conf, Seconds(3))
        ssc.sparkContext.setLogLevel("WARN")      
//            val lines2 = StreamingKafkaUtils.createDirectStream(ssc, brokers, Set(topic2), kafkaParams)
//  val lines1 = StreamingKafkaUtils.createDirectStream(ssc, brokers, Set(topic1), kafkaParams)
//       lines1.map{x=> println(111);Thread.sleep(3000);  x}.foreachRDD((rdd,time)=>{
//        rdd.collect().foreach{x=>  
//          val str = s"$time, rdd: $x"
//          println("line1 job1: "+str)  
//        }
//   })
//     StreamingKafkaUtils.updateOffset(lines1, kafkaParams)
//    val executor = Executors.newFixedThreadPool(6)
  val t1 =  new Thread(new Runnable() {
		def run() {
			val lines1 = StreamingKafkaUtils.createDirectStream(ssc, brokers, Set(topic1), kafkaParams)
        lines1.map{x=> println(111);Thread.sleep(3000);  x}.foreachRDD((rdd,time)=>{
         rdd.collect().foreach{x=>  
          val str = s"$time, rdd: $x"
          println("line1 job1: "+str)  
        }
   })
   
     StreamingKafkaUtils.updateOffset(lines1, kafkaParams)
		}
	})
  t1.start
  
  val t2=  new Thread(new Runnable() {
    def run() {
   val lines2 = StreamingKafkaUtils.createDirectStream(ssc, brokers, Set(topic2), kafkaParams)
 
      lines2.map{x=> println("line2...");Thread.sleep(3000);  x}.foreachRDD((rdd,time)=>{
      rdd.collect().foreach{x=>  
      val str = s"$time, rdd: $x"
      println("line2 job2: "+str)  
    }
   })
   
   StreamingKafkaUtils.updateOffset(lines2, kafkaParams)
    }
    })
  t2.start
  
  
  
//   lines1.map{x=> println(111);Thread.sleep(3000);  x}.foreachRDD((rdd,time)=>{
//    rdd.collect().foreach{x=>  
//      val str = s"$time, rdd: $x"
//      println("line1 job1: "+str)  
//    }
//   })
//   println("----------------------------------")
//    lines2.map{x=> println("line2...");Thread.sleep(3000);  x}.foreachRDD((rdd,time)=>{
//    rdd.collect().foreach{x=>  
//      val str = s"$time, rdd: $x"
//      println("line2 job2: "+str)  
//    }
//   })
   println("***********************************")
     
   
  Thread.sleep(3000)
    ssc.start()
    ssc.awaitTermination()
  }

}