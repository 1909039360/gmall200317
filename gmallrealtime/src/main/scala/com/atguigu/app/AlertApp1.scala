package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.common.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * Author: doubleZ
 * Datetime:2020/8/19   22:14
 * Description:
 */
object AlertApp1 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf和StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //2.读取Kafka 事件主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    //3.转换为样例类对象
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val evnetLogDStream: DStream[EventLog] = kafkaDStream.map(record => {
      record.value()
      //a.转换
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
      //b.处理日志日期和小时
      val date: Date = new Date(eventLog.ts)
      val dateHour: String = sdf.format(date)
      val timeArr: Array[String] = dateHour.split(" ")
      eventLog.logDate = timeArr(0)
      eventLog.logHour = timeArr(1)
      //c.返回结果
      eventLog
    })
    //4.开5min窗
    val windowsDStream: DStream[EventLog] = evnetLogDStream.window(Minutes(5))
    //5.按照mid做分组处理
    val groupEventDStream: DStream[(String, Iterable[EventLog])] = windowsDStream.map(eventLog => (eventLog.mid, eventLog)).groupByKey()
    //6.对单条数据做处理:
    //6.1 三次及以上用不同账号(登录并// 领取优惠劵)：对uid去重
    //6.2 没有浏览商品：反面考虑,如果有浏览商品,当前mid不产生预警日志
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = groupEventDStream.map { case (mid, logIter) => {
      //a.创建Set用于存放领券的UID
      val eventList: util.ArrayList[String] = new util.ArrayList()
      //创建List用于存放用户行为
      val uidSet: util.HashSet[String] = new util.HashSet[String]()
      val itemIds: util.HashSet[String] = new util.HashSet[String]()
      //定义标志位用于标识是否有浏览行为
      var viewFlag: Boolean = false
      //b.遍历logIter
      //提取事件类型
      breakable(
        logIter.foreach(eventLog => {
          //将事件添加至集合
          eventList.add(eventLog.evid)
          if ("coupon".equals(eventLog.evid)) //有领取优惠券的行为
          {
            uidSet.add(eventLog.mid)
            itemIds.add(eventLog.itemid)
          } else if ("clickItem".equals(eventLog.evid)) {
            viewFlag = true
            break()
          }
        })
      )
      if (uidSet.size() >= 3) {
        CouponAlertInfo(mid, uidSet, itemIds, eventList, System.currentTimeMillis())
      } else {
        null
      }
    }
    }
    // 预写日志测试打印
    val filterAlertDStream: DStream[CouponAlertInfo] = couponAlertInfoDStream.filter(x => x!=null)
    filterAlertDStream.cache()
    filterAlertDStream.print()
    //7.将生成的雨鞋日志写入ES
    filterAlertDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(iter =>{
        val docIDtoData: Iterator[(String, CouponAlertInfo)] = iter.map(alertInfo => {
          val minutes: Long = alertInfo.ts / 1000 / 60
          (s"${alertInfo.mid}-$minutes", alertInfo)
        })
        //获取当前时间
        val date: String = LocalDate.now().toString
        MyEsUtil.insertByBulk(GmallConstants.GMALL_ES_ALERT_INFO_PRE+"-"+date,
        "_doc",
          docIDtoData.toList)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
