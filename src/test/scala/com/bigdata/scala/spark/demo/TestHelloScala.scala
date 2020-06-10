package com.bigdata.scala.spark.demo

import com.bigdata.scala.spark.demo.streaming.JavaStreamingWCDemo
import org.scalatest.FunSuite


class TestHelloScala  extends FunSuite{

//  test("test_handleRecord_280m"){
//    HelloScala.main(Array("file:///C:\\Users\\it-out-allen\\Desktop\\deskTest\\records.json","4","16"));
//  }

  test("test SparkStreaming"){
//    StreamingWCDemo.main(Array("3","6","warn","5","192.168.41.1","18080"))
    JavaStreamingWCDemo.main(Array("3","6","info","5","192.168.41.1","18080"))

  }

}
