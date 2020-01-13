package com.leahy.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * spark streaming处理socket数据
 *
 * 在本地  nc -lk 6888
 */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[3]")

//    val ssc = new StreamingContext(sparkConf, Seconds(5))
//
//    val lines = ssc.socketTextStream("localhost", 6888)
//
//    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
//
//    result.print()

    val sc = new SparkContext(sparkConf)
    val listRDD = sc.makeRDD(List(1,2,3,4),2)
    val fileRDD = sc.textFile("in",3)


//    ssc.start()
//    ssc.awaitTermination()
  }

}