package com.atguigu.bigdata.gmall.publisher.service.impl;

import com.atguigu.bigdata.gmall.publisher.mapper.DauMapper;
import com.atguigu.bigdata.gmall.publisher.mapper.OrderMapper;
import com.atguigu.bigdata.gmall.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Witzel
 * @since 2019/7/22 10:28
 */
@Service
public class PublisherServiceImpl implements PublisherService {


    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;
    @Override
    public int getDauTotal(String date) {
        int dauTotal = dauMapper.getDauTotal(date);
        return dauTotal;
    }

    @Override
    public Map getDauHours(String date) {
        List<Map> dauHourList = dauMapper.getDauHour(date);
        // 把List<Map>结构转成Map结构
        Map dauHourMap = new HashMap();

        for (Map map : dauHourList) {
            // 此处获取的是查询结果的字段名loghour as LH ， count(*) as CT
            String loghour = (String)map.get("LH");
            Long ct = (Long)map.get("CT");
            dauHourMap.put(loghour, ct);

        }


        return dauHourMap;
    }


    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.getOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> orderAmountHoutList = orderMapper.getOrderAmountHout(date);
        Map orderAmountMap = new HashMap<>();
        for(Map map : orderAmountHoutList){
            orderAmountMap.put(map.get("CREATE_HOUR"), map.get("ORDER_AMOUNT"));
        }
        return orderAmountMap;
    }

}
