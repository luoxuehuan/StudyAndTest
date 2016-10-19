package cn.ibeifeng.spark

import org.apache.spark.sql.SparkSession

object StructuredNetworkWordCount {
  
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder()
        .appName("StructuredNetworkWordCount")  
        .getOrCreate()
        
        
    val myspark = SparkSession.builder().appName("d").getOrCreate()
    import spark.implicits._
    
    val lines = spark.readStream
        .format("socket")//指定流的格式
        .option("host", "localhost")  //设定参数
        .option("port", "9999")  
        .load()//load得到输入流
        
        
    /**
     * word是一个dataset
     */
    val words = lines.as[String].flatMap(_.split(" "))  
    
    
    /**
     * wordCount是一个dataframe
     */
    val wordCounts = words.groupBy("value").count() 
    
    val query = wordCounts.writeStream
        .outputMode("complete") //所有的
        .format("console")//输出到命令行
        .start()
        
    query.awaitTermination()  
  }
  
}