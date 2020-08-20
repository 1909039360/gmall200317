package com.atguigu.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.network.Send;

import java.util.Properties;
import java.util.function.Predicate;

/**
 * Author: doubleZ
 * Datetime:2020/8/18   11:35
 * Description:
 */
public class MyKafkaSender {
    //声明Kafka生产者
    public static KafkaProducer<String,String> kafkaProducer = null;
    //创建Kafka生产者的方法
    public static KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //注意下面的时序列化器 不是反序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String>producer =null;
        try{
            producer = new KafkaProducer<String, String>(properties);
        }catch (Exception e){
            e.printStackTrace();
        }
        return producer;
    }

    public static void send(String topic, String msg) {
        if(kafkaProducer == null){
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<>(topic,msg));
    }
}
class MyKafkaSender1{
    // 1 声明Kafka生产者
    private static KafkaProducer<String,String> kafkaProducer;
    // 2 创建kafka生产者方法 createKafkaProducer
    public static KafkaProducer<String,String> createKafkaProducer(){
        // 	1 创建 properties配置信息
        Properties properties = new Properties();
        // 	2 填写配置信息(bootstrap-servers key value 序列化器)
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        // 	3 创建kafkaProducer
        KafkaProducer<String, String> producer =null;
        try{
          producer=  new KafkaProducer<>(properties);
        }catch (Exception e){
            e.printStackTrace();
        }
        return producer;
    }
    // 3 创建kafka生产者发送消息方法 send(topic,msg)
    public static void send(String topic,String msg){
        if(kafkaProducer ==null){
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String,String>(topic,msg));
    }
}

class MyKafkaSender2{
    private static KafkaProducer<String,String> kafkaProducer =null;
    private static KafkaProducer<String,String> createKafkaProducer(){
        KafkaProducer<String,String> producer=null;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        try{
            producer = new KafkaProducer<String, String>(properties);
        }catch (Exception e){
            e.printStackTrace();
        }
        return  producer;
    }
    public static void send(String topic,String msg){
        if(kafkaProducer == null){
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String,String>(topic,msg));
    }

}
class MyKafkaSender3{
    private  static KafkaProducer<String,String> kafkaProducer = null;
    private  static KafkaProducer<String,String> createKafkaProducer(){
        KafkaProducer<String,String> producer = null;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        try{
            producer = new KafkaProducer<String, String>(properties);
        }catch (Exception e){
            e.printStackTrace();
        }
        return producer;
    }
    public static void send(String topic,String msg){
        if(kafkaProducer == null){
            kafkaProducer= createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String,String>(topic,msg));
    }


}
