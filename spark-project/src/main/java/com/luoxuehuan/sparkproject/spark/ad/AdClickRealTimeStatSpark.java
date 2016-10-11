package com.luoxuehuan.sparkproject.spark.ad;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.luoxuehuan.sparkproject.conf.ConfigurationManager;
import com.luoxuehuan.sparkproject.dao.IAdBlacklistDAO;
import com.luoxuehuan.sparkproject.dao.IAdUserClickCountDAO;
import com.luoxuehuan.sparkproject.dao.factory.DAOFactory;
import com.luoxuehuan.sparkproject.domain.AdUserClickCount;
import com.luoxuehuan.sparkproject.util.DateUtils;
import com.luoxuehuan.sparkproject.constant.Constants;
import com.luoxuehuan.sparkproject.domain.AdBlacklist;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class AdClickRealTimeStatSpark {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("add").setMaster("local[2]");
		
		JavaStreamingContext jssc = 
				new JavaStreamingContext(conf,new Duration(5));
		
		//jssc.checkpoint("hdfs://192.168.1.108:9090/checkpoint");
		/*
		 * kafka又基于Zookeeper! 3台机器
		 */
		Map<String,String> kafkaParams = new HashMap<String,String>();
		
		kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
				ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
		
		String KafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
		String[] kafkatopicssplited =KafkaTopics.split(",");
		/**
		 * 支持多个topic
		 */
		Set<String> topics = new HashSet<String>();
		for(String kafkatopic : kafkatopicssplited){
			topics.add(kafkatopic);
		}
		
		
		//基于kafka direct api模式 构建出针对kafka集群中指定topic 的输入DStream
		
		//String1,String2 1无实际意义,2代表kafaka topic 的一条一条实时日志数据 
		JavaPairInputDStream<String,String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
				jssc, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class, 
				kafkaParams, 
				topics);
		
		//一条一条的日志数据
		//timestamp provice city userid adid
		
		
		 JavaPairDStream<String, Long> dailyUserAdClickDStream = adRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(Tuple2<String, String> tuple)
					throws Exception {
				// 从tuple 中获取每一条原始的日志数据
				String log = tuple._2;
				String[] logSplited = log.split(" ");
				String timestamp = logSplited[0];
				Date date  = new Date(Long.valueOf(timestamp));
				String dateKey = DateUtils.formatDateKey(date);
				long userid = Long.valueOf(logSplited[3]);
				long adid = Long.valueOf(logSplited[4]);
				String key = dateKey ="_"+userid+"_"+adid;
				return new Tuple2<String,Long>(key,1L);
			}
		});
		 
		 /*
		  * reduce By Key 
		  * 就能得到每个batch 中每天time 每个用户userid对每个adid广告的点击量
		  */
		 JavaPairDStream<String, Long>  dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(new Function2<Long, Long, Long>() {
			
			@Override
			public Long call(Long v1, Long v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		 
		/*
		 * 到这里为止 获取到什么
		 * dailyUserAdClickCountDStream 
		 * 
		 * 源源不断的每隔5s 的batch中,当天每天每个用户对每个广告的点击数
		 * 
		 * 把数据累加到mysql中
		 */
		 
		 dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
				
				
				//对每个分区获取一次连接 而且是从连接池获取！
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
					
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
						
						List<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();
						
						while(iterator.hasNext()) {
							Tuple2<String, Long> tuple = iterator.next();
							
							String[] keySplited = tuple._1.split("_");
							String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
							// yyyy-MM-dd
							long userid = Long.valueOf(keySplited[1]);
							long adid = Long.valueOf(keySplited[2]);  
							long clickCount = tuple._2;
							
							AdUserClickCount adUserClickCount = new AdUserClickCount();
							adUserClickCount.setDate(date);
							adUserClickCount.setUserid(userid); 
							adUserClickCount.setAdid(adid);  
							adUserClickCount.setClickCount(clickCount); 
							
							adUserClickCounts.add(adUserClickCount);
						}
						
						IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
						adUserClickCountDAO.updateBatch(adUserClickCounts);  
						
						
					}
				});
				return null;
			}
		});
		
		 /*
		  * 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量。
		  * 
		  * 遍历每个batch中的所有记录，对每条记录都要去查询 一下  这个用户 对这个广告的点击量是多少
		  * 
		  * 从mysql查询
		  * 
		  * 查询出来的结果如果是100，则判定这个用户就是黑名单用户,就写入mysql表中。
		  * 
		  * 
		  * 
		  * 
		  * 
		  * 那么是从哪里开始过滤呢?使用哪个DStream呢?
		  * 
		  * 
		  * reducebykey的那个batch!!!!
		  * 
		  * 比如原始数据10000条,reduce之后只有5000条
		  * 
		  * 既可以满足需求，还能减少要处理的数据量
		  * 
		  * 
		  */
		 
		 JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(new Function<Tuple2<String,Long>, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Long> tuple) throws Exception {
				String key = tuple._1;
				String[] keySplited = key.split("_");  
				
				// yyyyMMdd -> yyyy-MM-dd
				String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));  
				long userid = Long.valueOf(keySplited[1]);  
				long adid = Long.valueOf(keySplited[2]);  
				
				// 从mysql中查询指定日期指定用户对指定广告的点击量
				IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
				int clickCount = adUserClickCountDAO.findClickCountByMultiKey(
						date, userid, adid);
				
				// 判断，如果点击量大于等于100，ok，那么不好意思，你就是黑名单用户
				// 那么就拉入黑名单，返回true
				if(clickCount >= 100) {
					return true;
				}
				// 反之，如果点击量小于100的，那么就暂时不要管它了
				return false;
			}
		});
		 
			// blacklistDStream
			// 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
			// 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
			// 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
			// 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
			// 所以直接插入mysql即可
			
			// 我们有没有发现这里有一个小小的问题？
			// blacklistDStream中，可能有userid是重复的，如果直接这样插入的话
			// 那么是不是会发生，插入重复的黑明单用户
			// 我们在插入前要进行去重
			// yyyyMMdd_userid_adid
			// 20151220_10001_10002 100  对广告10002 100次点击
			// 20151220_10001_10003 100  对广告10003 100次点击
			// 10001这个userid就重复了
			
			// 解决没有彻底去重！！！！！！！实际上，是要通过对dstream执行操作，对其中的rdd中的userid进行全局的去重
		 JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(
					
					new Function<Tuple2<String,Long>, Long>() {

						private static final long serialVersionUID = 1L;
			
						@Override
						public Long call(Tuple2<String, Long> tuple) throws Exception {
							String key = tuple._1;
							String[] keySplited = key.split("_");  
							Long userid = Long.valueOf(keySplited[1]);  
							return userid;
						}
						
					});
			
			JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(
					
					new Function<JavaRDD<Long>, JavaRDD<Long>>() {

						private static final long serialVersionUID = 1L;
			
						@Override
						public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
							return rdd.distinct();
						}
						
					});
			
			// 到这一步为止，distinctBlacklistUseridDStream
			// 每一个rdd，只包含了userid，而且还进行了全局的去重，保证每一次过滤出来的黑名单用户都没有重复的
			
			distinctBlacklistUseridDStream.foreachRDD(new Function<JavaRDD<Long>, Void>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Void call(JavaRDD<Long> rdd) throws Exception {
					
					rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {

						private static final long serialVersionUID = 1L;

						@Override
						public void call(Iterator<Long> iterator) throws Exception {
							List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();
							
							while(iterator.hasNext()) {
								long userid = iterator.next();
								
								AdBlacklist adBlacklist = new AdBlacklist();
								adBlacklist.setUserid(userid); 
								
								adBlacklists.add(adBlacklist);
							}
							
							IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
							adBlacklistDAO.insertBatch(adBlacklists); 
							
							// 到此为止，我们其实已经实现了动态黑名单了
							
							// 1、计算出每个batch中的每天每个用户对每个广告的点击量，并持久化到mysql中
							
							// 2、依据上述计算出来的数据，对每个batch中的按date、userid、adid聚合的数据
							// 都要遍历一遍，查询一下，对应的累计的点击次数，如果超过了100，那么就认定为黑名单
							// 然后对黑名单用户进行去重，去重后，将黑名单用户，持久化插入到mysql中
							// 所以说mysql中的ad_blacklist表中的黑名单用户，就是动态地实时地增长的
							// 所以说，mysql中的ad_blacklist表，就可以认为是一张动态黑名单
							
							// 3、基于上述计算出来的动态黑名单，在最一开始，就对每个batch中的点击行为
							// 根据动态黑名单进行过滤
							// 把黑名单中的用户的点击行为，直接过滤掉
							
							// 动态黑名单机制，就完成了
							
							// 第一套spark课程，spark streaming阶段，有个小案例，也是黑名单
							// 但是那只是从实际项目中抽取出来的案例而已
							// 作为技术的学习，（案例），包装（基于你公司的一些业务），项目，找工作
							// 锻炼和实战自己spark技术，没问题的
							// 但是，那还不是真真正正的项目
							
							// 第一个spark课程：scala、spark core、源码、调优、spark sql、spark streaming
							// 总共加起来（scala+spark）的案例，将近上百个
							// 搞通透，精通以后，1~2年spark相关经验，没问题
							
							// 第二个spark课程（项目）：4个模块，涵盖spark core、spark sql、spark streaming
							// 企业级性能调优、troubleshooting、数据倾斜解决方案、实际的数据项目开发流程
							// 大数据项目架构
							// 加上第一套课程，2~3年的spark相关经验，没问题
							
						}
						
					});
					
					return null;
				}
				
			});
		 
		 
		 //错误的做法！！！！！没有彻底对userid进行去重
//		 blacklistDStream.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {
//			
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
//
//				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
//
//					@Override
//					public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
//						
//						List<AdBlacklist> adBlackLists = new ArrayList<AdBlacklist>();
//						
//						
//						//将黑名单用户插入进去!
//						List<Long> userids = new ArrayList<Long>();
//						while(iterator.hasNext()){
//							Tuple2<String,Long> tuple = iterator.next();
//							String key = tuple._1;
//							String[] keySplited = key.split("_");
//							Long userid = Long.valueOf(keySplited[1]);
//							
//							if(!userids.contains(userid)){
//								userids.add(userid);
//								AdBlacklist adBlackList = new AdBlacklist();
//								adBlackList.setUserid(userid);
//								adBlackLists.add(adBlackList);
//							}
//						}
//						
//						/*
//						 * 思考?
//						 * 
//						 * 有没有彻底对userid进行去重
//						 * 
//						 * 因为只是对一个分区 进行了去重!!!!
//						 * 
//						 * 所以这个程序要改造一下?
//						 */
//					}
//				});
//				return null;
//			}
//		});
//		 
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
		

	}

}
