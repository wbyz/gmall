package com.atguigu.bigdata.gmall.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.bigdata.gmall.canal.hanlder.CanalHanlder;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author Witzel
 * @since 2019/7/23 8:50
 * 一个Canal的结构包含关系
 * Message（Entry（rowchange（rowdataList（rowdata（多个Column、Name、Value）））））
 */
public class CanalClient {
    public static void main(String[] args) {
        // 创建连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true){
            // 连接
            canalConnector.connect();
            // 抓取的表
            canalConnector.subscribe("gmall0218.*");
            // 抓取
            Message message = canalConnector.get(100);
            //
            if (message.getEntries().size()==0){
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    // 每一个entry对应一个sql
                    // 过滤entry，因为不是每个sql都是对数据修改的写操作,比如开关事务
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){
                        CanalEntry.RowChange rowChange = null;
                        try {
                            // 把storeValue反序列化 得到rowChange
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        // 获取行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalEntry.EventType eventType = rowChange.getEventType();//获取事件类型：insert update delete drop alter
                        //获取表名
                        String tableName = entry.getHeader().getTableName();

                        CanalHanlder canalHanlder = new CanalHanlder(tableName, eventType, rowDatasList);
                        canalHanlder.handle();

                    }
                }



            }
        }
    }
}
