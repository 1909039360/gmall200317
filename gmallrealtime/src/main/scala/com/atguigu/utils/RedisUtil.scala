package com.atguigu.utils

import java.util.Properties

import com.atguigu.utils.RedisUtil.jedisPool
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Author: doubleZ
 * Datetime:2020/8/15   21:04
 * Description:
 */
object RedisUtil{
  var jedisPool:JedisPool =_
  def getJedisClient :Jedis ={
    if(jedisPool == null){
      //
    //  println("开辟一个新的连接池")
      val config: Properties = PropertiesUtil.load("config.properties")
      val host: String = config.getProperty("redis.host")
      val port: String = config.getProperty("redis.port")

      val jedisPoolConfig: JedisPoolConfig = new  JedisPoolConfig
      jedisPoolConfig.setMaxTotal(100)//最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20)//最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)//忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true)//每次获得连接的进行测试
      jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt);
      println(s"jedisPool.getNumActive =${jedisPool.getNumActive}")
    }
    //println("获得一个连接")
    jedisPool.getResource
  }


}
object RedisUtil1{
  var jedisPool:JedisPool =_
  def getJedisClient()={
    if(jedisPool == null){
      val properties: Properties = PropertiesUtil.load("config.properties")
      val host: String = properties.getProperty("redis.host")
      val port: String = properties.getProperty("redis.port")
      val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
      jedisPoolConfig.setMaxTotal(100)
      jedisPoolConfig.setMaxIdle(20)
      jedisPoolConfig.setMinIdle(20)
      jedisPoolConfig.setBlockWhenExhausted(true)
      jedisPoolConfig.setMaxWaitMillis(100)
      jedisPoolConfig.setBlockWhenExhausted(true)

      jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
      println(s"jedisPool.getNumActive =${jedisPool.getNumActive}")
    }
    val resource: Jedis = jedisPool.getResource
    resource
  }

}