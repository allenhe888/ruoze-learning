package com.bigdata.scala.spark.demo

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.{JSON, JSONObject}

object RecordWindowAggr {
  var filePath = "hdfs:///test/input/records.json"
  var minPartitions= 6

  val timeWindowSize= 1000*60*5

  def main(args: Array[String]): Unit = {
    if(args.length >0){
      filePath = args(0)
    }else if(args.length>1){
      try{
        minPartitions = args(1).toInt
      }catch {
        case e:Exception => Unit
      }
    }
    val sc = getDefaultSpark().sparkContext
    sc.getConf.registerKryoClasses(Array(classOf[SimpleDateFormat]))

    println("1 进入 Coding: textFile(), cache并且collect")
    val hadoopRdd = sc.textFile(filePath,minPartitions)
    hadoopRdd
      .persist(StorageLevel.MEMORY_ONLY_SER)
      .collect()

    println("2 进入 Coding commPartHadoopRDD=mapPartitons(), cache并且collect ")
    val commPartHadoopRDD = hadoopRdd
      .mapPartitions(it=>{
      val timeWinSize= 300000
      val keyBuffer= scala.collection.mutable.Map[String, ArrayBuffer[(String,Double)]]()
      while (it.hasNext){
        try{
          val json = JSON.parseRaw(it.next())
          if(json.isDefined){
            val dataMap = json.get.asInstanceOf[JSONObject].obj
            val value = dataMap.get("value").get
            if(value.isInstanceOf[Double]){
              val time = dataMap.get("time").get.asInstanceOf[Double].toLong
              keyBuffer.getOrElseUpdate(((time/timeWinSize )*timeWinSize).toString,new ArrayBuffer[(String,Double)]())
                .+=((dataMap.get("pointId").get.toString,value.asInstanceOf[Double]))
            }
          }
        }catch {
          case e:ClassCastException => Unit
          case e:Exception => Unit
        }
      }
      keyBuffer.iterator
    })
      .persist(StorageLevel.MEMORY_ONLY_SER)
    commPartHadoopRDD.map(x=>{(x._1,x._2.size)}).collect().foreach(println)

    // 窗口聚合
    println("3 进入 Coding: reduceByKey().map(), printPartition() ")
    val job01Rdd = commPartHadoopRDD
      .reduceByKey((x,y)=>{x.++(y)})
      .map(x=>{
        val count=x._2.size
        val sum = x._2.map(_._2).sum
        val avg = sum /count
        val pointIds = x._2.map(_._1).toSet
        var pointStr= ""
        if(pointIds.size > 5){
          pointStr = pointIds.size.toString()
        }else{
          pointStr = pointIds.toString()
        }
        (x._1.toLong,count,sum,avg,pointStr)
      })
      .persist(StorageLevel.DISK_ONLY)
    printRDDInPartition(job01Rdd)


    // Job2: one Stage.
    println("4 进入 Coding:  flatMap().map().mapPartitons().map()  -> printPartition")
    val job02RDD= commPartHadoopRDD
      .flatMap(x=>{
        x._2.map(data=>{
          (x._1,data._1,data._2)
        })
      })
      .map( x=> (x._2, "time:"+ x._1+", value="+x._3))
      .mapPartitions(it=>{
        val map = scala.collection.mutable.HashMap[String,ArrayBuffer[String]]()
        it.foreach(x=>{
          map.getOrElseUpdate(x._1,new ArrayBuffer[String]()).+=(x._2)
        })
        map.iterator
      })
      .map(x=>{

        val curTimeStr = new SimpleDateFormat("yyyy年 mm分ss秒 SSS").format(new Date())
        val size = x._2.toList.size
        (x._1,size,curTimeStr)
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
    printRDDInPartition(job02RDD)

  }

  def getDefaultSpark():SparkSession={
    println("Welcome to Spark World! Allen He")
    try{
      Thread.sleep(1000)
    }catch {
      case e:Exception => e.printStackTrace()
    }

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()
//    spark.sparkContext.setLogLevel("WARN")
    spark
  }


  def printRDDInPartition(rdd:RDD[_]):Unit={
    System.out.println("\n****************************\n\n 打印 ( "+rdd.getClass.getSimpleName+"): " +rdd)
    rdd
      .mapPartitionsWithIndex((index,it)=>List((index+"_partition :\n\t"+it.mkString("\n\t"))).iterator)
      .foreach(x=>
        println(x))
  }


}
