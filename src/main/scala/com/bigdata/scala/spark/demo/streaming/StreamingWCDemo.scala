package com.bigdata.scala.spark.demo.streaming

import com.bigdata.scala.spark.demo.SparkHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWCDemo {


  case class MainArgs(cores:Int, minPartitions:Int, logLevel:String, batchInterval:Int, host:String, port:Int)

  val timeWindowSize= 1000*60*5
  var cores = 4
  var minPartitions = 6
  var logLevel = "info"
  var batchInterval = 3

  var host = "192.168.41.1"
  var port = 18080

  def parseMain(args: Array[String]): MainArgs = {

    if(args.length>0){
      cores = args(0).toInt
    }
    if(args.length>1){
      minPartitions = args(1).toInt
    }
    if(args.length>2){
      logLevel = args(2)
    }
    if(args.length>3){
      batchInterval = args(3).toInt
    }
    if(args.length>4){
      host = args(4)
    }
    if(args.length>5){
      port = args(5).toInt
    }

    return MainArgs(cores,minPartitions,logLevel,batchInterval,host,port)
  }

  def main(args: Array[String]): Unit = {
    val mainArgs:MainArgs = parseMain(args)

    val spark = SparkSession.builder()
      .appName("TestSpark")
      .master("local["+mainArgs.cores+"]")
      .getOrCreate()

    spark.sparkContext.setLogLevel(mainArgs.logLevel)

    val ssc = new StreamingContext(spark.sparkContext,Seconds(mainArgs.batchInterval))
    val lines = ssc.socketTextStream(mainArgs.host,mainArgs.port)

    SparkHelper.testStreamingWordCount(ssc,lines,mainArgs.minPartitions)

  }

}
