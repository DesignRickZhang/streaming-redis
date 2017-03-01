package cn.my.prefer

import org.apache.spark.scheduler.InputFormatInfo
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PreferedLoacationTest {

  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      sys.exit(0)
    }
    val Array(inputpath) = args
    val preferredLoacations = InputFormatInfo.computePreferredLocations(Seq(new InputFormatInfo(new Configuration(),
      classOf[TextInputFormat],inputpath)))
    val conf = new SparkConf()
    val sc  = new SparkContext(conf,preferredLoacations)
    val rdd = sc.textFile(inputpath)
    rdd.collect
    sc.stop
    }
}