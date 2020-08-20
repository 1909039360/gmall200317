package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.common.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * Author: doubleZ
 * Datetime:2020/8/20   14:33
 * Description:
 */
object GmvApp2 {
  def main(args: Array[String]): Unit = {
    //1 创建sparkConf StreamContext
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")setAppName("GMV")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    //2 获取kafka数据流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO,ssc)
    //3 处理数据 样例类 数据脱敏
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      //数据源为kv 类型的
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //添加 日期 时间
      val timeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = timeArr(0)
      orderInfo.create_hour = timeArr(1).split(":")(0)
      //数据脱敏 电话号中间位补*
      val consignee_tel: String = orderInfo.consignee_tel
      val telTuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple._1 + "************";

      orderInfo
    })
    orderInfoDStream.cache()
    orderInfoDStream.print()
    //4 写入数据 Phoenix
    orderInfoDStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix(
        "gmall200317_order_info",
        classOf[OrderInfo].getDeclaredFields.map(_.getName),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
