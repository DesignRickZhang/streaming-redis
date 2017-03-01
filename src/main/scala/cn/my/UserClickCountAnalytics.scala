package cn.my

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import cn.my.redis.RedisUtils
import org.apache.spark.streaming.kafka.utils.StreamingKafkaUtils
import scala.util.parsing.json.JSON

object UserClickCountAnalytics {
def main(args: Array[String]): Unit = {

//    var masterUrl = "local[2]"

    if (args.length < 3) {
      println("<masterUrl><broker><group>")
      sys.exit(0)
    }
      val Array(masterUrl, brokers,group) = args
 

    // Create a StreamingContext with the given master URL

    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    conf.set("spark.streaming.kafka.maxRatePerPartition","100")//生成一个blocck
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
 

    // Kafka configurations

    val topics = Set("user_events")


    val kafkaParams = Map[String, String](

      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
     "group.id" -> group)

 

    val dbIndex = 1

    val clickHashKey = "app::users::click"

 

    // Create a direct stream

  //  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
   
   val kafkaStream =  StreamingKafkaUtils.createDirectStream(ssc,brokers,topics,kafkaParams)

    val events = kafkaStream.flatMap(line => {

//      val data = JSONObject.fromObject(line._2)
      //Some(data)
    val b = JSON.parseFull(line._2)  
    b match {  
      case Some(map: Map[String, Any]) => Some(map)
    }
    })

 

    // Compute user click times
//import RedisUtils
    val userClicks = events.map(x => (x.get("uid").get.toString, x.get("click_count").get.toString.toDouble.toLong)).reduceByKey(_ + _)

    userClicks.foreachRDD((rdd,time)=>{
      rdd.foreachPartition(partitionsOfRecords =>{
        val jedis = RedisUtils.getJedis
        		
        partitionsOfRecords.foreach(pair =>{
          val uid = pair._1
          val clickCount = pair._2
          jedis.select(dbIndex)
          jedis.hincrBy(clickHashKey,uid,clickCount)
        }) 
        RedisUtils.returnJedis(jedis)
      })
    })
    

//1总耗时,2.print(sec)
    ssc.start()

    ssc.awaitTermination()

   
    }
}