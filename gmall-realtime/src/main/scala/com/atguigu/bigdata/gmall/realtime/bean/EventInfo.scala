package com.atguigu.bigdata.gmall.realtime.bean

/**
  * @author Witzel
  * @since 2019/7/23 20:19
  */
case class EventInfo(mid:String,
                     uid:String,
                     appid:String,
                     area:String,
                     os:String,
                     ch:String,
                     `type`:String,
                     evid:String ,
                     pgid:String ,
                     npgid:String ,
                     itemid:String,
                     var logDate:String,
                     var logHour:String,
                     var ts:Long
                    )

