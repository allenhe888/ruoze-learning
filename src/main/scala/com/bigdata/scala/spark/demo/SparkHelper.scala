package com.bigdata.scala.spark.demo

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer


object SparkHelper {

  @transient private var dataFormat:SimpleDateFormat = _

  def getDataFormat():SimpleDateFormat ={
    if (dataFormat == null){
      dataFormat = new SimpleDateFormat("yyyy年 mm分ss秒 SSS")
    }
    dataFormat
  }

  def commTest(sc: SparkContext,filePath:String, minPartitions:Int):Unit = {
    val hadoopRDD= sc.textFile(filePath,minPartitions)
    println("1 进入 hadoopRDD.mapPartitions, Memory_Only_Ser, 不提交")
    val paitGroupedRecords = hadoopRDD.mapPartitions(it=>{
      val timeWinSize= 300000
      val objKeyMap= scala.collection.mutable.Map[String, ArrayBuffer[JSONObject]]()
      while (it.hasNext){
        try{
          val json = JSON.parseObject(it.next())
          val value = json.getDouble("value")
          objKeyMap.getOrElseUpdate(json.getString("pointId"),new ArrayBuffer[JSONObject]()).+=(json)
        }catch {
          case e:ClassCastException => Unit
          case e:Exception => Unit
        }
      }
      objKeyMap.iterator
    })
      .persist(StorageLevel.MEMORY_ONLY_SER)

    // Job 0:  有shuffle操作
    println("2. 进入 Job 0: paitGroupedRecords.reduceByKey(),  printlnByPartition , 有shuffle操作")
    val avgRdd= paitGroupedRecords.reduceByKey((x,y)=>{x.++(y)})
      .map(x=>{
        val count=x._2.size
        val sum = x._2.map(_.getDouble("value").doubleValue()).sum
        val avg = sum /count
        val pointIds = x._2.map(_.getString("pointId")).toSet
        var pointStr= ""
        if(pointIds.size > 5){
          pointStr = pointIds.size.toString()
        }else{
          pointStr = pointIds.toString()
        }
        (x._1,count,sum,sum/count,pointStr,new SimpleDateFormat("yyyy年 mm分ss秒 SSS").format(new Date()))
      })
    printRDDInPartition(avgRdd)


    println("3. 进入 Job 1: paitGroupedRecords.reduceByKey(),  printlnByPartition , 仅map 转换")
    val flatRDD=  paitGroupedRecords.map(records=>{
      records._2.map(obj=>{
        obj.put("windowTime",records._1)
        obj
      })
      records
    })
      .flatMap(x=> x._2)
      .mapPartitionsWithIndex((idx,records)=>{
        records.map(x=> {
            x.put("partition", idx)
            x
          }
        )
      })
      .map(record=>{
        record.remove("orgId")
        record.remove("test")
        record.remove("modelId")
        record
      })
      .mapPartitions(it=>{
          val pointSet = it.map(_.getString("pointId")).toSet
          val size= it.toList.size
          List((size,pointSet)).toIterator
        })

    printRDDInPartition(flatRDD)


  }


  def printRDDInPartition(rdd:RDD[_]):Unit={
//    System.out.println("\n****************************\n\n 打印 ( "+rdd.getClass.getSimpleName+"): " +rdd)
    val result = rdd.mapPartitionsWithIndex((index,it)=>{
      if(it.size >0){
        val strList= List((index+"_partition :\n\t"+it.mkString("\n\t")))
        strList.foreach(x => println("RDD内部(executor)打印"+x))
        strList.iterator
      }else{
        List.empty.toIterator
      }
    })
    val out= result.collect()
    out.foreach(x=>{
      println(x)
    })

  }



  def getSocketDStream(sc:SparkContext,hostname:String,port:Int): ReceiverInputDStream[String]  ={
    val ssc = new StreamingContext(sc,Seconds(3))
    val lines = ssc.socketTextStream(hostname,port)
    lines
  }


  def getKafkaDStream(sc:SparkContext): ReceiverInputDStream[String]  ={
    val ssc = new StreamingContext(sc,Seconds(3))
    val lines = ssc.socketTextStream("192.168.41.141",19090)
    lines
  }

  def doDStreamProcess(ssc:StreamingContext, ds:ReceiverInputDStream[String], minPartitions:Int): Unit ={

    val words = ds.repartition(minPartitions).flatMap(_.split(" "))

    val partGroupedWords= words.mapPartitions(it=>{
      val map = new scala.collection.mutable.HashMap[String,ArrayBuffer[String]]
      it.foreach(word=>{
        map.getOrElseUpdate(word,new ArrayBuffer[String]()).+=(word)
      })
      map.iterator
    })

    val reducedWords = partGroupedWords.reduceByKey((x,y)=> x.++(y))
    reducedWords.persist(StorageLevel.MEMORY_ONLY_SER)

    // Job 0 : (Stage 0, Stage 1); {Stage 0, socketText()+ flatMap()+mapPartitions() }, { Stage 1 = reduceByKey() + mapPartitonsWithIndex() };
    reducedWords.foreachRDD(rdd=> printRDDInPartition(rdd))


    reducedWords.print()

    // Job 1:  map + reduce
    val windowRdd= reducedWords.map((x)=>{
      (x._1,x._2.length)
    })
      .mapPartitions(it=>{
        val curTime = System.currentTimeMillis()
        val window = (curTime/1000)*1000
        it.map(x=>(window,(x._1,x._2,curTime)))
      })
      .groupByKey().map(x => {
      val strTime = getDataFormat.format(new Date(x._1))
      (strTime,x._2.toString())
    })

    windowRdd.foreachRDD(rdd=> printRDDInPartition(rdd))


  }


  def testStreamingWordCount(ssc:StreamingContext, ds:ReceiverInputDStream[String], minPartitions:Int): Unit ={
    doDStreamProcess(ssc,ds,minPartitions)
    ssc.start()
    ssc.awaitTermination()

    println("ssc.await方法结束, 程序运行完成! ")
  }




}
