package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime}
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog, MyEsUtil}
import com.atguigu.common.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks
import scala.util.parsing.json.JSONObject

/**
 * Author: doubleZ
 * Datetime:2020/8/19   13:55
 * Description:
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf和StreamingContext
    val sparkConf: SparkConf = new SparkConf().setMaster("AlertApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    //2.读取Kafka 事件主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_EVENT,ssc)
    //3.转换为样例类对象
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventLogDStream: DStream[EventLog] = kafkaDStream.map(record => {
      //a.转换
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
      //b.处理日志日期和小时
      val timeStamp: Long = eventLog.ts
      val date: Date = new Date(timeStamp)

      val dateHour: String = sdf.format(date)
      val timeArr: Array[String] = dateHour.split(" ")
      eventLog.logDate = timeArr(0)
      eventLog.logHour = timeArr(1)
      //c.返回结果
      eventLog
    })
    //4.开5min窗
    val windowDStream: DStream[EventLog] = eventLogDStream.window(Minutes(5))
    //5.按照mid做分组处理
    val midToEventLogDStream: DStream[(String, Iterable[EventLog])] = windowDStream.map(eventlog =>(eventlog.mid,eventlog)).groupByKey()
    //6.对单条数据做处理:
    var noCLick:Boolean = false;
    val CouponAlertInfoDStream: DStream[CouponAlertInfo] = midToEventLogDStream.map { case (mid, logIter) => {
      //6.1 三次及以上用不同账号(登录并领取优惠劵)：对uid去重
      val uidSet: util.HashSet[String] = new util.HashSet[String]()
      val itemIds: util.HashSet[String] = new util.HashSet[String]()
      //创建LIst用于存放用户行为
      val events: util.ArrayList[String] = new util.ArrayList[String]()
      Breaks.breakable(
        logIter.foreach(eventLog => {
          val evid: String = eventLog.evid
          //将时间添加至集合
          events.add(evid)
          //判断当前数据是否为领券行为
          if ("coupon".equals(evid)) {
            itemIds.add(eventLog.itemid)
            uidSet.add(eventLog.uid)
          } else if ("clickItem".equals(evid)) {
            noCLick = true
            Breaks.break()
          }
        })
      )

      if (uidSet.size() >= 3) {
        CouponAlertInfo(mid, uidSet, itemIds, events, System.currentTimeMillis())
      } else {
        null
      }
    }
    }
    //预警日志测试打印
    val filterAlertDStream: DStream[CouponAlertInfo] = CouponAlertInfoDStream.filter(x => x!=null)
    filterAlertDStream.cache()
    filterAlertDStream.print()
    //7.将生成的预警日志写入ES
    filterAlertDStream.foreachRDD(rdd =>{
      //转换数据结构,预警日志 ===>(docID,预警日志)
      rdd.foreachPartition(iter =>{
        val docIdToData: Iterator[(String, CouponAlertInfo)] = iter.map(alertInfo => {
          val minutes: Long = alertInfo.ts / 1000 / 60
          (s"${alertInfo.mid}-$minutes", alertInfo)
        })
        //获取当前时间
        val date: String = LocalDate.now().toString
        MyEsUtil.insertByBulk(GmallConstants.GMALL_ES_ALERT_INFO_PRE+"-"+date,
        "_doc",
          docIdToData.toList)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
//1.创建SparkConf和StreamingContext
//2.读取Kafka 事件主题数据创建流
//3.转换为样例类对象
//a.转换
//b.处理日志日期和小时
//c.返回结果
//4.开5min窗
//5.按照mid做分组处理
//6.对单条数据做处理:
//6.1 三次及以上用不同账号(登录并领取优惠劵)：对uid去重
//6.2 没有浏览商品：反面考虑,如果有浏览商品,当前mid不产生预警日志
  //a.创建Set用于存放领券的UID
  //创建List用于存放用户行为
  //定义标志位用于标识是否有浏览行为
  //b.遍历logIter
//提取事件类型
//将事件添加至集合
//判断当前数据是否为领券行为
//判断当前数据是否为领券行为
//判断当前数据是否为领券行为
//判断当前数据是否为领券行为
//判断当前数据是否为领券行为