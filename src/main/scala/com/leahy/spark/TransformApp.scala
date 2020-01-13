package com.leahy.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TransformApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    /**
     * 构建黑名单
     */
    val blacks = List("zs","ls")
    val blackRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))

    val lines = ssc.socketTextStream("localhost", 6888)



     val clicklog = lines.map(x => (x.split(",")(1), x)).transform( rdd => {
      rdd.leftOuterJoin(blackRDD)
        .filter(x => x._2._2.getOrElse(false) != true)
        .map(x => x._2._1)
    })

      clicklog.print()

      ssc.start()
      ssc.awaitTermination()
  }

}

/*
输入格式:
20180808,zs
20180808,ls
20180808,ww

输出格式：
20180808,ww
 */

