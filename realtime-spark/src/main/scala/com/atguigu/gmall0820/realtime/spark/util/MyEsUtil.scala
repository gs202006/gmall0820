package com.atguigu.gmall0820.realtime.spark.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {
  private var factory: JestClientFactory = null;

  def getClient: JestClient = {
    if (factory == null) build();
    factory.getObject

  }

  def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(2000)
      .connTimeout(100000).readTimeout(100000).build())

  }

  def main(args: Array[String]): Unit = {

  }
  def saveData(): Unit = {
    val jest: JestClient = getClient
    //创建一个保存动作
    val index: Index = new Index.Builder(Movie("999", "急先锋"))
      .index("movie_test0820_2020-12-23").`type`("_doc").build()
    jest.execute(index)
    jest.close()
  }
  case class Movie(id: String, movie_name: String)
  //es的批量插入数据
  def saveBulkData(dataList: List[(Any,String)], indexName: String): Unit = {
    val jest: JestClient = getClient
    val builder = new Bulk.Builder
    if (dataList != null && dataList.size > 0) {
    try{
      for ((data,id) <- dataList) {
        val index: Index = new Index.Builder(data)
          .index(indexName).`type`("_doc").id(id).build()
        builder.addAction(index)
      }
      val bulk: Bulk = builder.build()


      val result: BulkResult = jest.execute(bulk)
      println("保存了  " + result.getItems.size() + "  条数据")
    }catch{
      case ex: Exception => {
        ex.printStackTrace() // 打印到标准err
        System.err.println ("exception===>: ...") // 打印到标准err
      }
    }
    }
//    val items: util.List[BulkResult#BulkResultItem] = result.getFailedItems
//    if(items.size()>0) {
//      println(items)
//    }
//    import  scala.collection.
//    for (item <- items ) {
//
//    }


      jest.close()


    //  }

  }
}
