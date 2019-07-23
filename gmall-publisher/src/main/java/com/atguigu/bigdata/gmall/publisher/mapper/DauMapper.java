package com.atguigu.bigdata.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author Witzel
 * @since 2019/7/22 9:47
 */
public interface DauMapper {
    public int getDauTotal(String date);

    public List<Map> getDauHour(String date);
}
