package com.atguigu.gmalllogger.controller;

/**
 * Author: doubleZ
 * Datetime:2020/8/14   19:47
 * Description:
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@RestController  = @Controller+方法上的@ResponseBody
@Slf4j
@RestController //Controller+responsebody
public class LoggerController {
    @Autowired
    //测试 我是second分支
     //测试 我是second分支
            // 测试 我是second分支
    KafkaTemplate<String,String> kafkaTemplate;
    @PostMapping("log")
    public String doLog(@RequestParam("logString") String logString ){
        // 0 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        // 1 落盘 file
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);
        // 2 推送到kafka
        if( "startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }
        return "success";
    }
    //测试 我是second分支
}
