package com.atguigu.gmall0820.realtime.spark.app


import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0820.realtime.spark.bean.DauInfo
import com.atguigu.gmall0820.realtime.spark.util.{MyEsUtil, MyKafkaUtil, OffsetManagerUtils, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}

object DauApp {
  def main(args: Array[String]): Unit = {
    //env
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dau_app_group"
    val topic = "ODS_BASE_LOG"
    //kafka  ODS_BASE_LOG
    // redis获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtils.getOffset(topic, groupId)
    //从kafka接收数据判断offset中是否有值，无->取kfk默认offset
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      println(offsetMap.size)
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

    //验证
    // recordInputStream.map(_.value()).print()
    //数据格式
    val jsonFormatDstream = offsetRecordInputStream.map { record =>
      val jsonObjec: JSONObject = JSON.parseObject(record.value())
      val ts = jsonObjec.getLong("ts")
      val format = new SimpleDateFormat("yyyy-MM-dd HH")
      val str = format.format(new Date(ts))
      val arrStrs = str.split(" ")
      jsonObjec.put("dt", arrStrs(0))
      jsonObjec.put("hr", arrStrs(1))
      jsonObjec
    }
    //用户首次访问页面
    val fVDstream = jsonFormatDstream.filter(js => {
      val pageJs = js.getJSONObject("page")
      var isFirst = false
      if (pageJs != null) {
        val lastPI = pageJs.getString("last_page_id")
        if (lastPI != null & lastPI != 0) {
          isFirst = true
        }
      }
      isFirst
    })
    //fVDstream.print(100)
    //去重
    val outDStream = fVDstream.mapPartitions { itr =>
      val jedis = RedisUtil.getJedisClient
      val itrToList = itr.toList
      println("before:" + itrToList.size)
      val listBuffer = new ListBuffer[JSONObject]()
      itrToList.foreach(one => {
        val mid = one.getJSONObject("common").getString("mid")
        val dt = one.getString("dt")

        val dauKey = "dau:" + dt
        val nonExist: lang.Long = jedis.sadd(dauKey, mid)
        //jedis.expire(dauKey, 24 * 3600)
        if (nonExist == 1L) {
          // println("空數據？")
          listBuffer.append(one)

        }
      })
      jedis.close()
      println("after:" + listBuffer.size)
      listBuffer.toIterator
    }

    outDStream.cache()
    outDStream.print(100)
    val dauInfoDStream: DStream[DauInfo] = outDStream.map { rdd => {
      val commonJSO: JSONObject = rdd.getJSONObject("common")
      //每次处理的是一个json对象   将json对象封装为样例类
      DauInfo(commonJSO.getString("mid"),
        commonJSO.getString("uid"),
        commonJSO.getString("ar"),
        commonJSO.getString("ch"),
        commonJSO.getString("vc"),
        commonJSO.getString("dt"),
        commonJSO.getString("hr"),
        System.currentTimeMillis())
    }
    }
    //输出数据
    dauInfoDStream.foreachRDD { rdd =>
      rdd.foreachPartition { dauItr =>
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        //通过增加id  来实现相同mid的幂等性
        val dateList: List[(DauInfo, String)] = dauItr.toList.map(dauInfo => (dauInfo, dauInfo.mid))
        MyEsUtil.saveBulkData(dateList.toList, "gmall0820_dau_info_" + dt)
        //1executor)
        //start
      }
      OffsetManagerUtils.saveOffset(topic, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
