package com.atguigu.bigdata.gmall.realtime.bean

/**
  * @author Witzel
  * @since 2019/7/24 8:35
  */
case class AlertInfo (mid:String,
                 uids:java.util.HashSet[String],
                 itemIds:java.util.HashSet[String],
                 events:java.util.List[String],
                 ts:Long)
