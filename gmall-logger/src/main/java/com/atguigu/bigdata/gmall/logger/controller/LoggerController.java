package com.atguigu.bigdata.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bigdata.gmall.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * @author Witzel
 * @since 2019/7/19 20:05
 */
@RestController //@Controller + @ResponseBody
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;


    // 在浏览器地址上输入并敲回车的是GET请求
//    @RequestMapping(path = "test",method = RequestMethod.GET)
//    @ResponseBody
    @GetMapping("test")     // 若确定是GET请求可以直接写GetMapping()
    public String getTest(){
        System.out.println("!!!!!!!!!!");
        return "success";
    }

    @PostMapping("log")
    public String doLog(@RequestParam("logString") String logString){
//        System.out.println(logString);

        // 1.加时间戳 先转成JSON
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        String jsonString = jsonObject.toJSONString();

        // 2.落盘工具 log4j /logback   此处选log4j
        log.info(jsonString);

        // 3.发送kafka
        // 判断是主题类型
        if("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, jsonString);
        } else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, jsonString);
        }

        return "success";
    }

}












