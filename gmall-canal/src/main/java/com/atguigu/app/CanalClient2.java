package com.atguigu.app;

import ch.qos.logback.classic.db.names.ColumnName;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.common.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.swing.plaf.ToolBarUI;
import java.awt.*;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Author: doubleZ
 * Datetime:2020/8/20   10:31
 * Description:
 */
public class CanalClient2 {
    public static void main(String[] args) {
        //获取Canal 连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");
        while(true){
            //连接
            canalConnector.connect();
            //订阅监控的表
            canalConnector.subscribe("gmall200317.*");
            //抓取数据
            Message message = canalConnector.get(100);
            //判断当前是否抓取到数据
            if(message.getEntries().size() <=0){
                System.out.println("当前抓取没有数据,休息一下");
                try{
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                //1.获取message中的Entry集合
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //2 获取entry中RowData 类型的数据
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if(CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())){
                        try{
                            //3.获取Entry中的表名和数据
                            String tableName = entry.getHeader().getTableName();
                            ByteString storeValue = entry.getStoreValue();
                            //4.反序列化storeValue
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            //5.获取事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //6.获取数据
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            //7.处理数据
                            handle(tableName,eventType,rowDatasList);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }

        }

    }
    //处理数据,根据表明以及时间类型将数据发送至Kafka指定主题
    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //GMV 需求,只需要order_info表中的新增数据  即 EventType 类型为 Insert 类型的
        if("order_infor".equals(tableName)&& CanalEntry.EventType.INSERT.equals(eventType)){
            sendToKafka(rowDatasList, GmallConstants.GMALL_TOPIC_USER_INFO);
         // order_detail 只会新增数据 不会修改数据 所以 只需要eventType 为 Insert的类型
        }else if("order_detail".equals(tableName) &&  CanalEntry.EventType.INSERT.equals(eventType)){
            sendToKafka(rowDatasList,GmallConstants.GMALL_TOPIC_ORDER_DETAIL);
            //user_info 即有新增 也有更新(数仓中的拉链表 缓慢变化维) 因此需要Insert  和 update 类型的数据
        }else if("user_info".equals(tableName)&&(CanalEntry.EventType.INSERT.equals(eventType)||
                CanalEntry.EventType.UPDATE.equals(eventType))){
            sendToKafka(rowDatasList,GmallConstants.GMALL_TOPIC_USER_INFO);
        }
    }

    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //创建一个JSON对象用于存放一行数据
            JSONObject jsonObject = new JSONObject();
            //遍历修改之后的列集
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            //打印单行数据并写入Kafka
            System.out.println(jsonObject.toString());
            MyKafkaSender.send(topic,jsonObject.toJSONString());
        }

    }
}
