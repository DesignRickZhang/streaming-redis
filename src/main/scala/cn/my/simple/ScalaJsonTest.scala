package cn.my.simple

import scala.util.parsing.json.JSON
import net.sf.json.JSONObject

object ScalaJsonTest {
  def main(args: Array[String]): Unit = {
    //val str2 = "{\"et\":\"kanqiu_client_join\",\"vtm\":1435898329434,\"body\":{\"client\":\"866963024862254\",\"client_type\":\"android\",\"room\":\"NBA_HOME\",\"gid\":\"\",\"type\":\"\",\"roomid\":\"\"},\"time\":1435898329}"  
    val str2 = "{\"uid\":\"4A4D769EB9679C054DE81B973ED5D768\",\"event_time\":\"1462009429259\",\"os_type\":\"Android\",\"click_count\":8}"
    val b = JSON.parseFull(str2)  
b match {  
  // Matches if jsonStr is valid JSON and represents a Map of Strings to Any  
  case Some(map: Map[String, Any]) =>
    println(map) ; 
    val clickCount = map.get("click_count").get
    println(s"clickCount: $clickCount")
    println("int:"+clickCount.toString().toDouble.toInt)
  case None => println("Parsing failed")  
  case other => println("Unknown data structure: " + other)  
  

}  
    val data = JSONObject.fromObject(str2)
    println("data:"+data)
    println("data:"+data.getString("uid"))  
  }
}