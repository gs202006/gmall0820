package com.atguigu.gmall0820.realtime.spark.app

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall0820.realtime.spark.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtils, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    //env
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "base_db_canal_app"
    val topic = "ODS_BASE_DB_C"

    //kafka  ODS_BASE_LOG
    // redis获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtils.getOffset(topic, groupId)
    //从kafka接收数据判断offset中是否有值，无->取kfk默认offset
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      println("-------------------" + offsetMap.size)
      recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
      println(recordInputStream)
    }

    // 转换为OffsetRange
    var offsetRanges = Array.empty[OffsetRange]
    val offsetRecordInputStream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform {
      dStream => {
        offsetRanges = dStream.asInstanceOf[HasOffsetRanges].offsetRanges
        println(offsetRanges(0).untilOffset + " untilOffset-----------")
        dStream
      }
    }
    //对从Kafka中读取到的数据进行结构转换，由Kafka的ConsumerRecord转换为一个Json对象

    val jsonObjDStream: DStream[JSONObject] = offsetRecordInputStream.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }
    //创建维度表集合
    val dimTables = Array("user_info", "sku_info", "base_province")
    jsonObjDStream.print(10)
    jsonObjDStream.foreachRDD { rdd =>
      rdd.foreachPartition { jsoObjItr =>
        val jedis: Jedis = RedisUtil.getJedisClient
        for (jsonObj <- jsoObjItr) {
          val tableName: String = jsonObj.getString("table")
          //处理维度表将其放入redis中  string pk json set
          if (dimTables.contains(tableName)) {
            val pkArray: JSONArray = jsonObj.getJSONArray("pkNames")
            val onePk: String = pkArray.getString(0)
            val dataArr: JSONArray = jsonObj.getJSONArray("data")
            import scala.collection.JavaConverters._
            for (data <- dataArr.asScala) {
              val jsonDataObj: JSONObject = data.asInstanceOf[JSONObject]
              val pkVal: String = jsonDataObj.getString(onePk)
              val key = "DIM:" + tableName.toUpperCase() + pkVal
              val value = jsonDataObj.toJSONString
              jedis.set(key, value)
            }
          } else {
            //事实表处理：发送到kfk的producer中
            val tableType: String = jsonObj.getString("type")
            var kind = ""
            if (tableType == "INSERT") {
              kind = "I"
            } else if (tableType == "UPDATE") {
              kind = "U"
            } else if (tableType == "DELETE") {
              kind = "D"
            }
            if (kind.size > 0) {
              val topic = "DWD_" + tableName.toUpperCase + "_" + kind
              import scala.collection.JavaConverters._
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              for (data <- dataArr.asScala) {
                val dataJsonObj: JSONObject = data.asInstanceOf[JSONObject]
                MyKafkaSink.send(topic, dataJsonObj.toJSONString)
              }
            }
          }
        }
        jedis.close()
      }
      OffsetManagerUtils.saveOffset(topic, groupId, offsetRanges)

    }
    // println(offsetRecordInputStream)
    //  offsetRecordInputStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
