package com.atguigu.bigdata.gmall.realtime.bean

/**
  * @author Witzel
  * @since 2019/7/20 14:12
  */
case class StartupLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long
                     )

