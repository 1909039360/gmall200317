package com.atguigu.bean

/**
 * Author: doubleZ
 * Datetime:2020/8/19   13:52
 * Description:
 */
case class EventLog(mid: String,
                    uid: String,
                    appid: String,
                    area: String,
                    os: String,
                    `type`: String,
                    evid: String,
                    pgid: String,
                    npgid: String,
                    itemid: String,
                    var logDate: String,
                    var logHour: String,
                    var ts: Long)