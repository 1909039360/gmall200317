package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.common.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Author: doubleZ
 * Datetime:2020/8/21   20:41
 * Description: 获取新增及修改用于并写入redis
 * 写入哪些信息 怎么写入
 */
object SaveUserInfoToRedisApp1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaveUserInfoToRedisApp1")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_USER_INFO,ssc)
    //将数据转换为json格式 写入到redis
    kafkaDStream.mapPartitions(iter=>{
      val jedisClient: Jedis = RedisUtil.getJedisClient
      iter.foreach(record =>{
        val userInfo: UserInfo = JSON.parseObject(record.value(),classOf[UserInfo])
        //设置redisKey UserInfo-userId
        val redisKey: String = s"UserInfo-${userInfo.id}"
        jedisClient.set(redisKey,userInfo.toString)
      })
      jedisClient.close()
      iter
    })

    kafkaDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(iter =>{
        val jedisClient: Jedis = RedisUtil.getJedisClient
        iter.foreach(record=>{
          val userInfo: UserInfo = JSON.parseObject(record.value(),classOf[UserInfo])
          val redisKey: String = s"userInfo-${userInfo.id}"
          jedisClient.set(redisKey,UserInfo.toString())
        })
        jedisClient.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
