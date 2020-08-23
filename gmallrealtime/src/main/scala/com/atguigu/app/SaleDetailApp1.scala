package com.atguigu.app

import java.time.LocalDate
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.common.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Author: doubleZ
 * Datetime:2020/8/21   20:57
 * Description: 双流jion 使用Redis作为缓存 保存延迟的数据(orderInfo orderDetail)
 *  //主要处理模块
 *  //双流jion
 *        orderInfo
 *        orderDetail
 *             orderID fullouterJion
 *             三种情况 第一 都能够匹配上 输出数据
 *                      第二  orderInfo 有值 orderDetail没有值
 *                                      到redis保存的 orderDetail 查找有没有对应的orderId
 *                      第三  orderDetail有值 orderInfo没有值
 *                                     到redis保存的orderId 查找有没有对应的orderInfo
 *                                      如果没有 保存到redis
 *                      无论orderId 是否都能够匹配上 都需要将 orderId保存到rediss 可以设置存活周期
 *            需要考虑的问题
 *                    orderInfo 保存到redis 用什么数据结构 (String)
 *                          redisKey 如何设计 value的数据如何保存
 *                          orderInfo:$orderId
 *                    orderDetail 保存到redis 用什么数据结果(set list 本例用用set) set ? 是否也有存活时间 是的 set也可以设置存活时间
 *                         redisKey 如果设计 value的数据如何保存 JSONObject.toString
 *                         orderDetail:${orderId}
 *  //user信息补充
 *        userInfo
 *        将userInfo保存到redis查询
 *        userinfo:userID value:JSONObject.toString
 *  //写入ES
 *        MyESUtils
 *          indexName 设计 ${}-$today
 *          docId 设计 使用 detail_id
 *          数据写出
 *
 */
object SaleDetailApp1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp1").setMaster()
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO,ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL,ssc)
    //处理orderInfo
    val orderIdToOrderDStream: DStream[(String, OrderInfo)] = orderDetailKafkaDStream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderInfo.create_date = orderInfo.create_date.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
      val consignee_tel: String = orderInfo.consignee_tel
      orderInfo.consignee_tel = consignee_tel.splitAt(4)._1 + "*********"
      (orderInfo.id, orderInfo)
    })
    //处理orderDetail
    val orderIdToDetailDstream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      (orderDetail.order_id, orderDetail)
    })
    // 进行双流join
    val infoJoinDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToOrderDStream.fullOuterJoin(orderIdToDetailDstream)
    // 对infoJoinDetailDStream:进行分支处理
    val noUserSaleDetail: DStream[SaleDetail] = infoJoinDetailDStream.mapPartitions(iter => {
      //通过mapPartitions 在分区中获取Jedis连接 减少获取连接次数
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //导入样例类对象转换为JSON的隐式
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
      val saleDetailList: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]
      iter.foreach { case (orderId, (infoOpt, detailOpt)) => {
        if (infoOpt.isDefined) {
          if (detailOpt.isDefined) {
            saleDetailList += new SaleDetail(infoOpt.get, detailOpt.get)
          } else {
            //获取detail的key
            val detailKey: String = s"orderDetail-${orderId}"
            //如果redis缓存中存在 合并到一起  这里的orderDetail 可能有多个 所以不能使用String  可以使用set 或者 list 这里使用set
            val orderDetailJsonSet: util.Set[String] = jedisClient.smembers(detailKey)
            if (!orderDetailJsonSet.isEmpty) {
              //将orderDetailJson转换为样例类对象
              orderDetailJsonSet.asScala.foreach(orderDetailJson => {
                //将orderDetailJson转换为样例类对象
                val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                //结合放入集合
                saleDetailList += new SaleDetail(infoOpt.get, orderDetail)
              })

            }
            //1.2 将orderInfo转换JSON字符串写入Redis
            // JSON.toJSONString(orderInfo) 编译报错 // 无论detailOpt 在不在都要写入redis
            var infoRedisKey: String = s"orderInfo-${detailOpt.get.order_id}"
            val orderInfoJSON: String = Serialization.write(infoOpt.get)
            jedisClient.set(infoRedisKey, orderInfoJSON)

          }
        } else if (detailOpt.isDefined) {
          val orderDetail: OrderDetail = detailOpt.get
          var infoRedisKey: String = infoOpt.get.id
          val orderInfoJson: String = jedisClient.get(infoRedisKey)
          if (orderInfoJson != null) { //缓存中 存在 orderIn 合并到一起
            //2.1 查询有数据
            // 将orderInfoJSon 转换为样例类对象
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            // 结合写出
            saleDetailList += new SaleDetail(orderInfo, orderDetail)
          } else {
            //不存在 写到缓存中
            // 查询没有结果,将当前的orderDetail写入Reids
            var detailRedisKey: String = s"orderDetail:${detailOpt.get.order_id}"
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailRedisKey, orderDetailJson)
            jedisClient.expire(detailRedisKey, 100)
          }
        }

      }
      }
      jedisClient.close()
      saleDetailList.toIterator
    })
    //TODO 根据userID 查询Redis数据,将用户信息补充完整
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetail.mapPartitions(iter => {
      //a.获取Redis 连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.遍历iter,对每一条数据进行查询Redis操作,补充用户信息
      val details: Iterator[SaleDetail] = iter.map(noUserSaleDetail => {
        //查询Redis
        val useInfoJson: String = jedisClient.get(s"userInfo:${noUserSaleDetail.user_id}")
        //将userInfoJson 转换为样例类
        val userInfo: UserInfo = JSON.parseObject(useInfoJson, classOf[UserInfo])
        //补充信息
        noUserSaleDetail.mergeUserInfo(userInfo)
        //返回数据
        noUserSaleDetail
      })
      //c.释放连接
      jedisClient.close()
      //e.返回数据
      details
    })

    //TODO 将三张表JOIN的结果写ES
    saleDetailDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(iter =>{
        //根据当天时间创建索引名称
        val today: String = LocalDate.now.toString
        val indexName: String = s"${GmallConstants.GMALL_ES_SALE_DETAIL_PRE}-$today"
        //将orderDetailiID作为ES中索引的docId
        val detailIDtoSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map(saleDetail =>(saleDetail.order_detail_id,saleDetail))
        //调用ES工具写入数据
        //调用ES 工具类写入数据
        MyEsUtil.insertByBulk(indexName,"_doc",detailIDtoSaleDetailIter.toList)
      })
    })

  }

}
