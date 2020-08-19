package com.atguigu.bean

import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

/**
 * Author: doubleZ
 * Datetime:2020/8/19   14:31
 * Description:
 */
object MyEsUtil {
  private val ES_HOST:String = "http://hadoop102"
  private val ES_HTTP_PORT:Int = 9200
  private var factory:JestClientFactory =_

  def getClient:JestClient ={
    if(factory == null)  bulid()
    factory.getObject()
  }
  def close(client :JestClient): Unit ={
    if(!Objects.isNull(client)) {
      try{
        client.hashCode()
      }catch {
        case e:Exception =>
          e.printStackTrace()
      }
    }
  }
  private def bulid() ={
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST+":"+ES_HTTP_PORT)
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
        .readTimeout(10000)
        .build()
    )
  }
  def insertByBulk(indexName:String,typeName:String,list:List[(String,Any)])={
    //集合非空
    if(list.nonEmpty){
      //a.获取链接
      val client: JestClient = getClient
      //创建Bulk.Builder对象
      val builder: Bulk.Builder = new Bulk.Builder()
        .defaultIndex(indexName)
        .defaultType(typeName)
      //c.遍历list 创建Index对象 并放入Bulk.Builder对象
      list.foreach{case (docId,data)=>{
        //给每一条数据创建Index对象
        val index: Index = new Index.Builder(data).id(docId).build()
        //将index 放入builder对象中
        builder.addAction(index)
      }}
      //d.Bulk.Builder创建buld
      val bulk: Bulk = builder.build()
      //e.执行写入数据操作
      client.execute(bulk)
      //f.释放链接
      close(client)

    }
  }


}


















