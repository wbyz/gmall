package com.atguigu.bigdata.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bigdata.gmall.common.constant.GmallConstant
import com.atguigu.bigdata.gmall.realtime.bean.StartupLog
import com.atguigu.bigdata.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

/**
  * @author Witzel
  * @since 2019/7/20 17:21
  */
object DauApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        // 1 消费kafka
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

        /*// 先打印数据看看
        inputDStream.foreachRDD(rdd=>{
            println(rdd.map(_.value()).collect().mkString("\n"))
        })*/

        // 2 结构转换成case class 补充两个时间字段
        val startupLogDStream: DStream[StartupLog] = inputDStream.map{ record =>
            val jsonString: String = record.value()
            val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])
            val formatter = new SimpleDateFormat("yyyy-MM-dd HH")
            val datetimeString: String = formatter.format(new Date(startupLog.ts))
            val datetimeArr: Array[String] = datetimeString.split(" ")

            startupLog.logDate = datetimeArr(0)
            startupLog.logHour = datetimeArr(1)

            startupLog
        }

        // 因为最后有两个分串操作，得先cache缓存一下，断了前面的关联
        startupLogDStream.cache()

        // 3 去重 根据今天访问过的用户清单进行过滤
        val filteredDStream: DStream[StartupLog] = startupLogDStream.transform { rdd =>
            println("过滤前" + rdd.count())

            val jedisClient: Jedis = RedisUtil.getJedisClient
            val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
            val dateString: String = dateFormat.format(new Date())
            val key: String = "dau:" + dateString
            val midSet: util.Set[String] = jedisClient.smembers(key)

            jedisClient.close()

            val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)

            val filteredRDD: RDD[StartupLog] = rdd.filter(startupLog => {
                !midBC.value.contains(startupLog.mid)
            })

            println("过滤后" + filteredRDD.count())
            filteredRDD
        }
        // 本批次内进行去重
        val distinctDStream: DStream[StartupLog] = filteredDStream.map(startupLog => (startupLog.mid, startupLog)).groupByKey().flatMap { case (mid, startupLogItr) =>
            startupLogItr.take(1)
        }


        /* // 问题1：连接操作jedis次数过多
        filteredDStream.filter(startupLog=>{
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val key: String = "dau:" + startupLog.logDate
            !jedisClient.sismember(key ,startupLog.mid)
        })*/


        /* // 问题2：没有周期性查询redis,只执行了一次
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val dateString: String = dateFormat.format(new Date())
        val key: String = "dau:" + dateString
        val midSet: util.Set[String] = jedisClient.smembers(key)
        val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)

       startupLogDStream.filter(startupLog => {
            !midBC.value.contains(startupLog.mid)
        })*/



        // 4 把所有今天访问过的用户保存起来
        distinctDStream.foreachRDD(rdd=>{
            rdd.foreachPartition(startupItr=>{  // 利用foreachPartition减少连接次数
                val jedisClient: Jedis = RedisUtil.getJedisClient
                for (startupLog <- startupItr) {
                    val key: String = "dau:"+startupLog.logDate
                    jedisClient.sadd(key,startupLog.mid)
//                    println(startupLog)
                }
                jedisClient.close()
            })
        })


        //把数据写入hbase+phoenix
        distinctDStream.foreachRDD{rdd=>
            rdd.saveToPhoenix("GMALL0218_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
        }

        ssc.start()
        ssc.awaitTermination()

    }
}
