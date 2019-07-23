package com.atguigu.bigdata.gmall.publisher.service;

import java.util.Map;

/**
 * @author Witzel
 * @since 2019/7/22 9:54
 */
public interface PublisherService {
    /**
     * 查询日活总数
     * @param date
     * @return
     */
    public int getDauTotal(String date );

    /**
     * 查询日活分时明细
     * @param date
     * @return
     */
    public Map getDauHours(String date );

    // 查询总交易额
    public Double getOrderAmountTotal(String date);

    // 查询分时交易额
    public Map getOrderAmountHour(String date);

}
