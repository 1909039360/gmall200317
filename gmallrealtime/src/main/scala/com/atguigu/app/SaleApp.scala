package com.atguigu.app

import java.time.LocalDate
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
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
 * Datetime:2020/8/21   11:44
 * Description:
 */
object SaleApp {
  def main1(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO,ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL,ssc)
    //  转换样例类
    val orderInfoDStream= orderInfoKafkaDStream.map(record => {
      val orderInfoStr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      orderInfo.create_date = orderInfo.create_time.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
      val consignee_tel: String = orderInfo.consignee_tel
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple._1+"***********"
      (orderInfo.id,orderInfo)
    })
    val orderDetailDStream = orderDetailKafkaDStream.map(record => {
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      (orderDetail.order_id,orderDetail)
    })
    val orderIdToInfoAndDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)
    //TODO 使用mapPartitions代替map操作,减少连接的创建于释放
    val noUserSaleDetail: DStream[SaleDetail] = orderIdToInfoAndDetailDStream.mapPartitions(iter => {
      //获取Redis 连接
      implicit val formats=org.json4s.DefaultFormats
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放Join上(包含当前批次,以及两个流中跟前置批次JOIN上的)的数据
      val details: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]
      //导入样例类对象转换为JSON的隐式
      iter.foreach { case ((orderId, (infoOpt, detailOpt))) => {

        //定义info & detail数据存入Redis中的Key
        val infoRedisKey: String = s"order_info:$orderId"
        val detailRedisKey: String = s"order_detail:$orderId"
        //1.infoOpt有值
        if (infoOpt.isDefined) {
          //获取infoOpt中的数据
          val orderInfo: OrderInfo = infoOpt.get
          //1.1 detailOpt也有值
          if (detailOpt.isDefined) {
            //获取detailOpt中的数据
            val orderDetail: OrderDetail = detailOpt.get
            //结合放入集合
            details += new SaleDetail(orderInfo, orderDetail)
          }
          //1.2 将orderInfo转换JSON字符串写入redis
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, orderInfoJson)
          jedisClient.expire(infoRedisKey, 500) // 设置redis中的存活时间
          //1.3 查询OrderDetail流前置批次数据
          val orderDetailJSonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
          orderDetailJSonSet.asScala.foreach(orderDetailJson => {
            //将orderDetailJson转换为样例类对象
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            //结合存放入集合
            details += new SaleDetail(orderInfo, orderDetail)
          })
        } else {
          //2.infoOpt 没有值
          // 获取detailOpt中的数据
          val orderDetail: OrderDetail = detailOpt.get
          //查询OrderInfo流前置批次数据
          val orderInfoJson: String = jedisClient.get(infoRedisKey)
          if (orderInfoJson != null) {
            //2.1 查询有数据
            //将orderInfoJson转换为样本类对象
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            details += new SaleDetail(orderInfo, orderDetail)
          } else {
            //2.2 查询没有结果,将当前的orderDetail写入Redis
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailRedisKey, orderDetailJson)
            jedisClient.expire(detailRedisKey, 100);
          }
        }

      }
      }
      jedisClient.close()
      details.toIterator
    })
    //TODO 打印测试
    noUserSaleDetail.print(100)
    //TODO 根据userID查询数据,将用户信息补充完整
    //TODO 根据userID查询Redis数据,将用户信息补充完整
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetail.mapPartitions(iter => {

      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //b.遍历iter,对每一条数据进行查询Redis操作,补充用户信息
      val details: Iterator[SaleDetail] = iter.map(noUserSaleDetail => {
        //查询Redis
        val userInfoJson: String = jedisClient.get(s"userInfo:${noUserSaleDetail.user_id}")
        //将userInfoJson转换为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
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
    //TODO 将三张表JOIN的结果写入ES
    saleDetailDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(iter=>{
        //根据当天时间创建索引名称
        val today: String = LocalDate.now.toString
        val indexName: String = s"${GmallConstants.GMALL_ES_SALE_DETAIL_PRE}-$today"
        //将orderDetailID作为ES中索引的docID
        val detailIdTOSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map(saleDetail => (saleDetail.order_detail_id,saleDetail))
        //调用ES工具类写入数据
      //处理分区中每一条数据
        MyEsUtil.insertByBulk(indexName,"_doc",detailIdTOSaleDetailIter.toList)
      }
      )
    })

    //TODO 启动任务
    ssc.start()
    ssc.awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO,ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL,ssc)
    //  转换样例类
    val orderInfoDStream= orderInfoKafkaDStream.map(record => {
      val orderInfoStr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      orderInfo.create_date = orderInfo.create_time.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
      val consignee_tel: String = orderInfo.consignee_tel
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple._1+"***********"
      (orderInfo.id,orderInfo)
    })
    val orderDetailDStream = orderDetailKafkaDStream.map(record => {
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      (orderDetail.order_id,orderDetail)
    })
    val orderIdToInfoAndDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)
    //TODO 使用mapPartitions代替map操作,减少连接的创建于释放
    val noUserSaleDetail: DStream[SaleDetail] = orderIdToInfoAndDetailDStream.mapPartitions(iter => {
      //获取Redis 连接
      implicit val formats=org.json4s.DefaultFormats
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放Join上(包含当前批次,以及两个流中跟前置批次JOIN上的)的数据
      val details: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]
      //导入样例类对象转换为JSON的隐式
      iter.foreach { case ((orderId, (infoOpt, detailOpt))) => {

        //定义info & detail数据存入Redis中的Key
        val infoRedisKey: String = s"order_info:$orderId"
        val detailRedisKey: String = s"order_detail:$orderId"
        //1.infoOpt有值
        if (infoOpt.isDefined) {
          //获取infoOpt中的数据
          val orderInfo: OrderInfo = infoOpt.get
          //1.1 detailOpt也有值
          if (detailOpt.isDefined) {
            //获取detailOpt中的数据
            val orderDetail: OrderDetail = detailOpt.get
            //结合放入集合
            details += new SaleDetail(orderInfo, orderDetail)
          }
          //1.2 将orderInfo转换JSON字符串写入redis
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, orderInfoJson)
          jedisClient.expire(infoRedisKey, 100) // 设置redis中的存活时间
          //1.3 查询OrderDetail流前置批次数据
          val orderDetailJSonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
          orderDetailJSonSet.asScala.foreach(orderDetailJson => {
            //将orderDetailJson转换为样例类对象
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            //结合存放入集合
            details += new SaleDetail(orderInfo, orderDetail)
          })
        } else {
          //2.infoOpt 没有值
          // 获取detailOpt中的数据
          val orderDetail: OrderDetail = detailOpt.get
          //查询OrderInfo流前置批次数据
          val orderInfoJson: String = jedisClient.get(infoRedisKey)
          if (orderInfoJson != null) {
            //2.1 查询有数据
            //将orderInfoJson转换为样本类对象
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            details += new SaleDetail(orderInfo, orderDetail)
          } else {
            //2.2 查询没有结果,将当前的orderDetail写入Redis
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailRedisKey, orderDetailJson)
            jedisClient.expire(detailRedisKey, 100);
          }
        }

      }
      }
      jedisClient.close()
      details.toIterator
    })
    //TODO 打印测试
    noUserSaleDetail.print(100)
    //TODO 根据userID查询数据,将用户信息补充完整
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetail.mapPartitions(iter => {
      //查询Redis
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.遍历iter,每一条数据进行查询Redis操作,补充用户信息
      val details: Iterator[SaleDetail] = iter.map(noUserSaleDetail => {
        //查询Redis
        val userInfoJson: String = jedisClient.get(s"userInfo:${noUserSaleDetail.user_id}")
        //将userInfoJson转换为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
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
    //TODO 将三张表JOIN的结果写入ES
    //TODO 将三张表JOIN的结果写入ES
    //TODO 将三张表JOIN的结果写入ES
    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        //根据当天时间创建索引名称
        val today: String = LocalDate.now().toString
        val indexName: String = s"${GmallConstants.GMALL_ES_SALE_DETAIL_PRE}-$today"
        //将orderDetailId作为ES中索引的docId
        val detailIdToSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map(saleDetail => (saleDetail.order_detail_id, saleDetail))
        //调用ES工具类写入数据  每个分区 批量写入一次数据
        MyEsUtil.insertByBulk(indexName, "_doc", detailIdToSaleDetailIter.toList)
      })
    })

    //TODO 启动任务
    ssc.start()
    ssc.awaitTermination()
  }

}
