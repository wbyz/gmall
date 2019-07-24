package com.atguigu.bigdata.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bigdata.gmall.common.constant.GmallConstant
import com.atguigu.bigdata.gmall.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.bigdata.gmall.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
  * @author Witzel
  * @since 2019/7/23 20:14
  */
object AlertApp {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alert_indo")

        val ssc = new StreamingContext(sparkConf, Seconds(5))

        //        println("开始消费kafka")
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT, ssc)

        val eventInfoDStream: DStream[EventInfo] = inputDStream.map(record => {
            val jsonString: String = record.value()
            val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
            eventInfo
        })

        // 1 同一设备 --> group by
        val groupbyMidDStream: DStream[(String, Iterable[EventInfo])] = eventInfoDStream.map(eventInfo => (eventInfo.mid, eventInfo)).groupByKey()

        // 2 5分钟内(课堂演示30秒) 滑动窗口 窗口大小 滑动步长
        val windowDStream: DStream[(String, Iterable[EventInfo])] = groupbyMidDStream.window(Seconds(30), Seconds(5))

        // 打标记 是否满足预警条件 整理格式 整理成预警格式
        // 3 三次及以上用不同账号登录并领取优惠券
        // 4 领券过程中没有浏览商品
        val checkAlertInfoDStream: DStream[(Boolean, AlertInfo)] = windowDStream.map {
            case (mid, eventInfoItr) => {
                val couponUidSet = new util.HashSet[String]()
                val itemIdsSet = new util.HashSet[String]() //商品ID集合
                val eventInfoList = new util.ArrayList[String]() //用户行为集合

                var clickItemFlag: Boolean = false
                breakable(

                    for (eventInfo: EventInfo <- eventInfoItr) {
                        eventInfoList.add(eventInfo.evid)
                        if (eventInfo.evid == "coupon") { //  如果有领优惠券的行为记录，就在领券清单中添加用户id，在商品清单中添加商品id
                            //                        println(eventInfo.uid + " || " + eventInfo.itemid)
                            couponUidSet.add(eventInfo.uid)
                            itemIdsSet.add(eventInfo.itemid)
                        } else if (eventInfo.evid == "clickItem") { // 如果有浏览商品的行为记录，就将标记改为true,并且跳出循环
                            clickItemFlag = true
                            break
                        }
                    }
                )

                val alertInfo = AlertInfo(mid, couponUidSet, itemIdsSet, eventInfoList, System.currentTimeMillis())

                (couponUidSet.size() >= 3 && !clickItemFlag, alertInfo)

            }
        }

        // 5 过滤不满足预警条件的数据
        val alertInfoDStream: DStream[AlertInfo] = checkAlertInfoDStream.filter(_._1 == true).map(_._2)

        // 6 去重 两个窗口之间有多个重复数据 可以依靠存储容器进行去重
        // （用redis ，可以直接用set集合）
        // （设置主键：mysql主键重复会报错，es主键重复会直接覆盖原数据）
        // 利用es的主键进行去重 主键（mid+分钟） 确保每分钟相同mid只有一条预警
        // 7 保存到es中
        alertInfoDStream.foreachRDD(rdd => {
            rdd.foreachPartition(alterInfoItr => {

                val alterList: List[(String, AlertInfo)] = alterInfoItr.toList.map(alterInfo => ((alterInfo.mid + "_" + alterInfo.ts / 1000 / 60), alterInfo))
                MyEsUtil.insertBulk(GmallConstant.EX_INDEX_ALERT, alterList)
            })
        })


        ssc.start()
        ssc.awaitTermination()
    }
}
