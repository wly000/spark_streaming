package com.leahy.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用Spark Streaming完成词频统计
 *
 * 本机： nc -lk 6888
 */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6888)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    //TODO 将统计结果写入到MySQL
    result.foreachRDD( rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /*
  ** 获取MySQL的链接
   */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_spark?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8", "root", "123456")

  }
}
