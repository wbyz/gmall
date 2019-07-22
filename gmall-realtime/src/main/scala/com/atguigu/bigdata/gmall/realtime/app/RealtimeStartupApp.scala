package com.atguigu.bigdata.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.bigdata.gmall.common.constant.GmallConstant
import com.atguigu.bigdata.gmall.realtime.bean.StartupLog
import com.atguigu.bigdata.gmall.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Witzel
  * @since 2019/7/20 14:12
  */
object RealtimeStartupApp {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall")
        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(10))

        val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

        //       startupStream.map(_.value()).foreachRDD{ rdd=>
        //         println(rdd.collect().mkString("\n"))
        //       }

        val startupLogDstream: DStream[StartupLog] = startupStream.map(_.value()).map { log =>
            // println(s"log = ${log}")
            val startUpLog: StartupLog = JSON.parseObject(log, classOf[StartupLog])
            startUpLog
        }
    }
}
