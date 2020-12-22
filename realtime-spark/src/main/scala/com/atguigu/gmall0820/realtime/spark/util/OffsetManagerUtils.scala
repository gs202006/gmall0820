package com.atguigu.gmall0820.realtime.spark.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManagerUtils {
  //在redis中保存offset
  def saveOffset(topic: String, group: String, offsetRanges: Array[OffsetRange]): Unit = {
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey = topic + ":" + group
    //将offset放入redis的hashset中 hash  key : topic+group   field:partition   value: offset
    val offsetVMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    for (offsetRange <- offsetRanges) {
      //OffsetRange结构[分区号，fromOffset，untilOffset]
      val pt: String = offsetRange.partition.toString
      val offsetV: String = offsetRange.untilOffset.toString
      //测试
      println("写入偏移量：分区"+pt +":"+offsetRange.fromOffset+"-->"+offsetV)
      offsetVMap.put(pt,offsetV)
    }
    //批量写入redis
    jedis.hmset(offsetKey,offsetVMap)
    jedis.close()

  }


  //获取偏移量offset
  def getOffset(topic: String, group: String): Map[TopicPartition, Long] = {

    val je: Jedis = RedisUtil.getJedisClient
    val offsetKey = topic + ":" + group
    //查询redis中的offset
    val offsetMap: util.Map[String, String] = je.hgetAll(offsetKey)
    //转换为kafka中的偏移量结构
    import scala.collection.JavaConverters._
    val topicParMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (p, offset) =>
        val topicParti: TopicPartition = new TopicPartition(topic, p.toInt)
        (topicParti, offset.toLong)
    }.toMap
    //测试
    for ( offset<- topicParMap ) {
      println("加载偏移量：" + offset)
    }
    //println("加载偏移量：" + topicParMap)

    topicParMap

  }
}
