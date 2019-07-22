package com.atguigu.bigdata.gmall.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
  * @author Witzel
  * @since 2019/7/20 14:10
  */
object PropertiesUtil {

    def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtil.load("config.properties")

        println(properties.getProperty("kafka.broker.list"))
    }

    def load(propertieName:String): Properties ={
        val prop=new Properties();
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
        prop
    }

}

