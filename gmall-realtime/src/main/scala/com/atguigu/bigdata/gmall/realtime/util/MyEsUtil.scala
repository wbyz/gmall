package com.atguigu.bigdata.gmall.realtime.util

import java.util
import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}

/**
  * @author Witzel
  * @since 2019/7/24 11:01
  */
object MyEsUtil {
    private val ES_HOST = "http://hadoop102"
    private val ES_HTTP_PORT = 9200
    private var factory: JestClientFactory = null

    /**
      * 获取客户端
      *
      * @return jestclient
      */
    def getClient: JestClient = {
        if (factory == null) build()
        factory.getObject
    }

    /**
      * 关闭客户端
      */
    def close(client: JestClient): Unit = {
        if (!Objects.isNull(client)) try
            client.shutdownClient()
        catch {
            case e: Exception =>
                e.printStackTrace()
        }
    }

    /**
      * 建立连接
      */
    private def build(): Unit = {
        factory = new JestClientFactory
        factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
                .maxTotalConnection(20) //连接总数
                .connTimeout(10000).readTimeout(10000).build)

    }

    // batch/bulk 批量
    // 批量插入
    def insertBulk(indexName: String, list: List[(String, Any)]): Unit = {
        if (list.size > 0) {

            val jest: JestClient = getClient

            val bulkBuilder = new Bulk.Builder()
            bulkBuilder.defaultIndex(indexName).defaultType("_doc")

            for ((id, doc) <- list) {
                // 先构造成为index（插入操作）
                val index: Index = new Index.Builder(doc).id(id).build()
                bulkBuilder.addAction(index) // 把多个插入操作保存到bulk中
            }

            val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems

            println("保存 " + items.size() + " 条数据！")

            close(jest)
        }


        def main(args: Array[String]): Unit = {
            val jest: JestClient = getClient

            val index: Index = new Index.Builder(Stud("wangwu", "sh")).index("gmall0218_alert_info1").`type`("_doc").id("4").build()
            jest.execute(index)

            close(jest)
        }
    }

    case class Stud(name: String, nickname: String)

}


