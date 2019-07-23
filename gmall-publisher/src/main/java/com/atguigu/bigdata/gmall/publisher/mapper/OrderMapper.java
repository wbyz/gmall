package com.atguigu.bigdata.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author Witzel
 * @since 2019/7/23 9:43
 */
public interface OrderMapper {
    public Double getOrderAmountTotal(String date);

    public List<Map> getOrderAmountHour(String date);
}
