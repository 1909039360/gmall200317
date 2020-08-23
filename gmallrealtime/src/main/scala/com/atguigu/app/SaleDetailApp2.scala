package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.common.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization
import org.json4s.DefaultFormats

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Author: doubleZ
 * Datetime:2020/8/23   10:55
 * Description:
 */
object SaleDetailApp2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp2").setMaster("local[*]")
    val ssc : StreamingContext = new StreamingContext(sparkConf,Seconds(6))
    val orderInfoDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO,ssc)
    val orderDetailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL,ssc)
    //处理样例列 并转换为(k,v)结果  用于fullOuterJoin
    val orderInfoToIdDStream: DStream[(String, OrderInfo)] = orderInfoDStream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderInfo.create_date = orderInfo.create_time.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
      val consignee_tel: String = orderInfo.consignee_tel
      orderInfo.consignee_tel = consignee_tel.splitAt(4)._1 + "****************"
      (orderInfo.id, orderInfo)
    })
    val orderDetailToIdDStream: DStream[(String, OrderDetail)] = orderDetailDStream.map(record => {
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      (orderDetail.order_id, orderDetail)
    })
    //双流join
    val orderInfoAndDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoToIdDStream.fullOuterJoin(orderDetailToIdDStream)
    //对是否join上进行判断处理
    //因为需要连接redis 使用mapPartitions 代替map 减少连接此处
    implicit val formats=org.json4s.DefaultFormats
    val saleDetailDStream: DStream[SaleDetail] = orderInfoAndDetailDStream.mapPartitions(iter => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val result: ArrayBuffer[SaleDetail] = new ArrayBuffer[SaleDetail]
      iter.map { case (orderId, (info, detail)) => {
        //info detail redisKey
        val infoRedisKey: String = s"orderInfo:${info.get.id}"
        val orderDetailRedisKey: String = s"orderDetail:${detail.get.order_id}"
        if (info.isDefined) { // info 不为空
          //1 orderInfo 写入 redis
          val orderInfo: OrderInfo = info.get
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, orderInfoJson)
          jedisClient.expire(infoRedisKey, 100) //设置存活时间 这里容易忘
          //2 orderDetail 不为空 封装结果 写出到ES  这里是先收集起来 然后添加userInfo信息 最后写出到ES
          if (detail.isDefined) {
            //写出到result
            val orderDetail: OrderDetail = detail.get
            val infoDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            result += infoDetail
          } else {
            //3 orderDetail 为空 到ES 查询是否有结果 有结果将结果收集起来
            val orderDetailSet: util.Set[String] = jedisClient.smembers(orderDetailRedisKey)
            orderDetailSet.asScala.foreach(detailJson => {
              val orderDetail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
              val infoDetail: SaleDetail = new SaleDetail(info.get, orderDetail)
              result += infoDetail
            })
          }
        } else if (detail.isDefined) { //detail 不为空 此时 info是为空的数据
          val orderDetail: OrderDetail = detail.get
          //1 到redis中 查询是否有对应的orderInfo
          val orderInfoJson: String = jedisClient.get(s"orderInfo:${orderDetail.order_id}")
          if (orderInfoJson != null) {
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val infoDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            result += infoDetail
          } else {
            //2 没有的化 将detail 写出到redis
            val orderDetailJsonStr: String = Serialization.write(orderDetail)
            jedisClient.sadd(orderDetailRedisKey, orderDetailJsonStr)
          }
        }


      }
      } //注意这里的map 是可以没有返回值的

      jedisClient.close()
      result.toIterator
    })

    //给 saleDetailDStream 添加 userInfo 数据
    //mapPartitions
    //获取jedisClient
    val saleDetailwithUserInfoDStram: DStream[SaleDetail] = saleDetailDStream.mapPartitions(iter => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val details: Iterator[SaleDetail] = iter.map(saleDetail => {
        var userInfoRedisKey = s"userInfo:${saleDetail.user_id}"
        val userInfoJson: String = jedisClient.get(userInfoRedisKey)
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        //年龄如何计算 当前日期减去 生日
        val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val birthday: Date = format.parse(userInfo.birthday)
        val cur: Long = System.currentTimeMillis()
        val betweenMs: Long = cur - birthday.getTime
        var age = betweenMs / 1000L / 60L / 60L / 24L / 365L
        saleDetail.user_age = age.toInt
        saleDetail.user_gender = userInfo.gender
        saleDetail.user_level = userInfo.user_level
        saleDetail
      })
      jedisClient.close()
      details
    })

    //写出到ES
    saleDetailwithUserInfoDStram.foreachRDD(rdd =>{
      rdd.foreachPartition(iter =>{
        //根据当天时间创建索引名称
        val today: String = LocalDate.now.toString
        val indexName: String = s"${GmallConstants.GMALL_ES_SALE_DETAIL_PRE}-$today"
        //将orderDetailID作为ES中索引的docID
        val detailIdToSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map(saleDetail =>(saleDetail.order_detail_id,saleDetail))
        //调用ES工具类写入数据 每个分区 批量写入一次数据
        MyEsUtil.insertByBulk(indexName,"_doc",detailIdToSaleDetailIter.toList)
      })
      //foreachRDD 没有返回值的transFrom 转换rdd后可以直接使用rdd的算子
      //注意即使使用foreachRDD foreachRDD 中的代码也会只执行一次
    saleDetailwithUserInfoDStram.foreachRDD(rdd =>{
      rdd.foreachPartition(iter =>{
        val today: String = LocalDate.now.toString
        var indexName =s"${GmallConstants.GMALL_ES_SALE_DETAIL_PRE}-$today"
        //转换结构 将orderDetailID 作为ES 中索引的docID
        val detailIdToSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        //调用ES工具类写入数据,注意每个分区 批量写入一次数据
        MyEsUtil.insertByBulk(indexName,"_doc",detailIdToSaleDetailIter.toList)
      })
    })

    })
    ssc.start()
    ssc.awaitTermination()
  }
}
