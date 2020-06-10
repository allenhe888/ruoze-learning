package com.bigdata.scala.spark.demo


import org.apache.spark.sql.SparkSession

object HelloScala {
  var filePath = "file:///C:\\Users\\it-out-allen\\Desktop\\deskTest\\records_20m.json"
  var minPartitions = 6
  var cores = 4
  val timeWindowSize= 1000*60*5
  def main(args: Array[String]): Unit = {
    if(args.length>2){
      filePath = args(0)
      cores = args(1).toInt
      minPartitions = args(2).toInt
    }else if(args.length>1){
      filePath = args(0)
      cores = args(1).toInt
    }else if(args.length >0){
      filePath = args(0)
    }


    val spark = SparkSession.builder()
      .appName("TestSpark")
//      .master("local["+cores+"]")
      .master("yarn")
      .config("deploy-mode","cluster")
      .getOrCreate()

    val sc = spark.sparkContext
//    sc.getConf.registerKryoClasses(Array(classOf[JSON],classOf[JSONObject]))
//    sc.getConf.registerKryoClasses(Array(classOf[SimpleDateFormat]))
//    sc.getConf.set("spark.serializer", classOf[KryoSerializer].getName)
//    sc.getConf.set("spark.executor.memory", "1G")
//    sc.getConf.set("spark.executor.instances", "2")

    SparkHelper.commTest(sc,filePath,minPartitions)

  }


}
