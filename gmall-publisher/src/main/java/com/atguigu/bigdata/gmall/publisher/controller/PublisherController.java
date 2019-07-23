package com.atguigu.bigdata.gmall.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.bigdata.gmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Witzel
 * @since 2019/7/22 9:49
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    /**
     * 查询总数
     * @param date
     * @return
     */
    @GetMapping("realtime-total")
   public String getRealTimeTotal(@RequestParam("date") String date){

        List<Map> totalList = new ArrayList<>();

               Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        int dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);


        Map newMidMap = new HashMap();
        newMidMap.put("id", "dau");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        totalList.add(newMidMap);

        Map orderAmountMap = new HashMap();
        orderAmountMap.put("id", "order_amount");
        orderAmountMap.put("name", "新增交易额");
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);
        orderAmountMap.put("value", orderAmountTotal);
        totalList.add(orderAmountMap);

       return JSON.toJSONString(totalList);
   }

   @GetMapping("realtime-hour")
   public String getRealtimeHour(@RequestParam("id")String id,@RequestParam("date") String todayDate){
        if(id.equals("dau")){
            // 日活
            Map dauHourTDMap = publisherService.getDauHours(todayDate);
            String yesterdayDate = getYDate(todayDate);
            Map dauHourYDMap = publisherService.getDauHours(yesterdayDate);

            Map<String,Map> hourMap = new HashMap<>();
            hourMap.put("today", dauHourTDMap);
            hourMap.put("yesterday", dauHourYDMap);

            return JSON.toJSONString(hourMap);

        } else if (id.equals("order_amount")){
            // 交易额
            Map orderHourTDMap = publisherService.getOrderAmountHour(todayDate);
            String yesterdayDate = getYDate(todayDate);
            Map orderHourYDMap = publisherService.getOrderAmountHour(yesterdayDate);

            Map<String,Map> hourMap = new HashMap<>();
            hourMap.put("today", orderHourTDMap);
            hourMap.put("yesterday", orderHourYDMap);

            return JSON.toJSONString(hourMap);

        }


        return  null;
   }


   private String getYDate(String todayDate){
       SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
       String ydateString="";
       try {
           Date tdate = simpleDateFormat.parse(todayDate);
           Date ydate = DateUtils.addDays(tdate, -1);
           ydateString = simpleDateFormat.format(ydate);
       } catch (ParseException e) {
           e.printStackTrace();
       }

       return ydateString;
   }

   //
}
