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
import org.codehaus.jackson.map.ext.JodaDeserializers.LocalDateDeserializer

import scala.util.control.Breaks


/**
 * Author: doubleZ
 * Datetime:2020/8/20   15:19
 * Description:
 */
object AlertApp2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp2").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_ES_ALERT_INFO_PRE,ssc)
    val sdf : SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventLogDStream: DStream[EventLog] = kafkaDStream.map(record => {
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
      val date: Date = new Date(eventLog.ts)
      val dateHour: String = sdf.format(date)
      eventLog.logDate = dateHour.split(" ")(0)
      eventLog.logHour = dateHour.split(" ")(1)
      eventLog
    })
    eventLogDStream.cache()
    eventLogDStream.print()
    // 处理重复的数据
    //开窗 5分钟 处理5分钟之内的数据
    val windowDStream: DStream[EventLog] = eventLogDStream.window(Minutes(5))
    //按照 midID 进行分组
    val midGroupDStream: DStream[(String, Iterable[EventLog])] = windowDStream.map(x =>(x.mid,x)).groupByKey()
    //5.按照mid做分组处理
    //6.对单条数据做处理:
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = midGroupDStream.map { case (mid, iterEventLog) => {
      //6.1 三次及以上用不同账号(登录并领取优惠劵)：对uid去重
      //6.2 没有浏览商品：反面考虑,如果有浏览商品,当前mid不产生预警日志
      //a.创建Set用于存放领券的UID
      val uidSet: util.HashSet[String] = new util.HashSet[String]()
      //b.创建set用于存放浏览过的商品id
      val itemsSet: util.HashSet[String] = new util.HashSet[String]()
      //创建List用于存放用户行为
      val eventsList: util.ArrayList[String] = new util.ArrayList[String]()
      var no_click: Boolean = false
      Breaks.breakable(
        iterEventLog.foreach(eventLog => {
          if ("coupon".equals(eventLog.evid)) {
            uidSet.add(eventLog.uid)
            itemsSet.add(eventLog.itemid)
          } else if ("clickItem".equals(eventLog.evid)) {
            no_click = true
            Breaks.break()
          }
          eventsList.add(eventLog.evid)
        })
      )

      if (uidSet.size() >= 3) {
        CouponAlertInfo(mid, uidSet, itemsSet, eventsList, System.currentTimeMillis())
      } else {
        null
      }
    }
    }
    couponAlertInfoDStream.cache()
    couponAlertInfoDStream.print()
    //注意这里写入前需要将空置过滤掉
    val filterAlertDStream: DStream[CouponAlertInfo] = couponAlertInfoDStream.filter(x => x!=null)
    //7.将生成的预警日志写入ES
    filterAlertDStream.foreachRDD( rdd =>{
      rdd.foreachPartition(iter =>{
        val docIDtoData: Iterator[(String, CouponAlertInfo)] = iter.map(alterInfo => {
          val minutes: Long = alterInfo.ts / 1000 / 60
          (s"${alterInfo.mid}-$minutes", alterInfo)
        })
        //获取当前日期
        val date: String = LocalDate.now.toString
        MyEsUtil.insertByBulk(GmallConstants.GMALL_ES_ALERT_INFO_PRE+"-"+date,
        "_doc",
          docIDtoData.toList)
      })
    })



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
//预警日志测试打印
//7.将生成的预警日志写入ES
//获取当前时间