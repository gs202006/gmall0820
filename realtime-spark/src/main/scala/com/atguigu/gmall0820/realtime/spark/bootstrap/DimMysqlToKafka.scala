package com.atguigu.gmall0820.realtime.spark.bootstrap

import java.util

import com.alibaba.fastjson.JSONObject
import com.atguigu.gmall0820.realtime.spark.util.{MyKafkaSink, MySqlUtil}

object DimMysqlToKafka {
  def main(args: Array[String]): Unit = {
    // read mysql
    val dimTables = Array("base_province","sku_info")
    //读取msq
    for (table <- dimTables) {
      val rsList: util.List[JSONObject] = MySqlUtil.queryList("select * from " + table)
      //对应kafka从canal接收数据的数据结构
      //table &查询结果&pkNames&type
      val canalJSONObj = new JSONObject()
      canalJSONObj.put("data", rsList)
      canalJSONObj.put("table", table)
      canalJSONObj.put("type", "INSERT")
      canalJSONObj.put("pkNames",Array("id"))

      println(canalJSONObj.toJSONString)
      MyKafkaSink.send("ODS_BASE_DB_C",canalJSONObj.toJSONString)
    }

    //write to kafka
  }
}
