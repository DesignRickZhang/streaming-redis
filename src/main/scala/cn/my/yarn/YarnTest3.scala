package cn.my.yarn
import org.apache.hadoop.yarn.client.api._
import org.apache.hadoop.conf.Configuration

object YarnTest3 {
  def main(args: Array[String]): Unit = {
      val yarnClient = YarnClient.createYarnClient();
      val conf = new Configuration()
      yarnClient.init(conf);
      yarnClient.start();
      
      
  }
}