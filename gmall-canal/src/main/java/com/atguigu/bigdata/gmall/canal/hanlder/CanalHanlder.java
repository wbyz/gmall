package com.atguigu.bigdata.gmall.canal.hanlder;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.bigdata.gmall.canal.util.MyKafkaSender;
import com.atguigu.bigdata.gmall.common.constant.GmallConstant;


import java.util.List;

/**
 * @author Witzel
 * @since 2019/7/23 11:57
 */
public class CanalHanlder {

    String tableName;
    CanalEntry.EventType eventType;
    List<CanalEntry.RowData> rowDataList;

    public CanalHanlder(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }

    public void handle(){
            // 下单操作
        if(tableName.equals("order_info")&&eventType==CanalEntry.EventType.INSERT){
            // 遍历行集
            for (CanalEntry.RowData rowData : rowDataList) {
                // 修改后的行集
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                // 遍历列集
                for (CanalEntry.Column column : afterColumnsList) {
                    System.out.println(column.getName()+" || "+column.getValue());
                    jsonObject.put(column.getName(), column.getValue());
                }

                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER, jsonObject.toJSONString()); // 发送kafka

            }
        }


    }
}
