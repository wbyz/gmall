package com.atguigu.bigdata.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.bigdata.gmall.common.constant.GmallConstant
import com.atguigu.bigdata.gmall.realtime.bean.OrderInfo
import com.atguigu.bigdata.gmall.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author Witzel
  * @since 2019/7/23 9:14
  */
import org.apache.phoenix.spark._
object OrderApp {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")

        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

        // 整理 转换
        val orderInfoDStream: DStream[OrderInfo] = inputDStream.map(record => {

            val jsonString: String = record.value()
            println(jsonString)
            // 转换成 case class
            val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
            // 脱敏 电话号码 1381*******
            val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
            orderInfo.consignee_tel = telTuple._1 + "*******"
            // 补充日期
            val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
            orderInfo.create_date = datetimeArr(0) // 日期
            //
            val timeArr: Array[String] = datetimeArr(1).split(":")
            orderInfo.create_hour = timeArr(0) //小时

            // 4 练习需求 增加一个字段 在订单表，表示该笔订单是否是该用户的首次下单 1表示首次下单，0表示非首次下单
            // 1） 维护一个清单，每次新用户下单，要追加入清单
            // 2） 利用清单进行标识，
            // 3） 同时还要查询本批次是否有多次下单 //要注意状态信息表的大小，来决定存储容器，redis<mysql（<es<hbase ：这两个写入查询速度偏慢，对实时要求不太好）

            orderInfo
        })

        // 保存到hbase + phoenix
        orderInfoDStream.foreachRDD(rdd=>{
            rdd.saveToPhoenix("GMALL0218_ORDER_INFO",
                Seq("ID",
                    "PROVINCE_ID",
                    "CONSIGNEE",
                    "ORDER_COMMENT",
                    "CONSIGNEE_TEL",
                    "ORDER_STATUS",
                    "PAYMENT_WAY",
                    "USER_ID",
                    "IMG_URL",
                    "TOTAL_AMOUNT",
                    "EXPIRE_TIME",
                    "DELIVERY_ADDRESS",
                    "CREATE_TIME",
                    "OPERATE_TIME",
                    "TRACKING_NO",
                    "PARENT_ORDER_ID",
                    "OUT_TRADE_NO",
                    "TRADE_BODY",
                    "CREATE_DATE",
                    "CREATE_HOUR"),
                new Configuration,
                Some("hadoop102,hadoop103,hadoop104:2181"))
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
