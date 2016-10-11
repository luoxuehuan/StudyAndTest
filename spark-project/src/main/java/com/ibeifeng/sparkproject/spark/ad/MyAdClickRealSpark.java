package com.ibeifeng.sparkproject.spark.ad;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;


import scala.Tuple2;

public class MyAdClickRealSpark {

	public static void main(String[] args) {
		/**
		 * 
		 */
		SparkConf conf  = new SparkConf();
		conf.setAppName("adapp");
		conf.setMaster("local");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
			
		
		/**
		 * 思路
		 * 1： 读取文件
		 * 2： 切分文件flat map
		 * 3：map to pair 映射成pair（word,1）
		 * 4:单词计数(word,N)
		 */
		JavaRDD<String> file = jsc.textFile("e://txt//testdata.txt");
		
		
		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				//List<Stirng> wordlist = 
						
				return Arrays.asList(line.split(" "));
				
			}
		});
		
		
		JavaPairRDD<String, Long>  wordPair = words.mapToPair(new PairFunction<String, String, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(String word) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String,Long>(word,1L);
			}
		});
		
		
		JavaPairRDD<String, Long> wordcount = wordPair.reduceByKey(new Function2<Long, Long, Long>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		
		
		wordcount.foreach(new VoidFunction<Tuple2<String,Long>>() {
			
			@Override
			public void call(Tuple2<String, Long> tuple) throws Exception {
				System.out.println("单词:"+tuple._1 + "个数："+tuple._2);
				
			}
		});
		
		
		
		 JavaPairRDD<Long, String>  sortPair = wordcount.mapToPair(new PairFunction<Tuple2<String,Long>, Long, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<String, Long> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long, String>(tuple._2,tuple._1);
			}
		});
		
		sortPair.sortByKey(false).foreach(new VoidFunction<Tuple2<Long,String>>() {

			@Override
			public void call(Tuple2<Long, String> tuple) throws Exception {
				System.out.println("个数:"+tuple._1 + "单词："+tuple._2);
				
			}
		});;
		
		jsc.close();
	}

}
