package com.luoxuehuan.sparkproject.spark.page;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSONObject;
import com.luoxuehuan.sparkproject.constant.Constants;
import com.luoxuehuan.sparkproject.dao.IPageSplitConvertRateDAO;
import com.luoxuehuan.sparkproject.dao.ITaskDAO;
import com.luoxuehuan.sparkproject.dao.factory.DAOFactory;
import com.luoxuehuan.sparkproject.domain.PageSplitConvertRate;
import com.luoxuehuan.sparkproject.domain.Task;
import com.luoxuehuan.sparkproject.util.DateUtils;
import com.luoxuehuan.sparkproject.util.NumberUtils;
import com.luoxuehuan.sparkproject.util.ParamUtils;
import com.luoxuehuan.sparkproject.util.SparkUtils;

import scala.Tuple2;

/**
 * 页面单跳转化率模块spark作业
 * @author lxh
 *
 */
public class PageOneStepConvertRateSpark {

	public static void main(String[] args) {
		/*
		 * 1、构造Spark上下文
		 */
		SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
		SparkUtils.setMaster(conf);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
		
		
		
		
		
		/*
		 * 2、生成模拟数据 本地才会mock数据
		 * 
		 * 	生产环境直接从hive中查数据
		 */
		SparkUtils.mockData(sc, sqlContext);
		
		/*
		 * 3.查询获取任务的参数
		 */
		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
		
		taskid =3L;
		System.out.println("taskid:"+taskid);
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskid);
		System.out.println("taskparm:"+task.getTaskParam());
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");  
			return;
		}

		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		/*
		 * 查询指定日期范围内的用户访问行为数据
		 */
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		
		
		/*
		 * 对用户访问行为数据做一个映射，将其映射为<Sessionid,访问行为>的格式
		 * 用户访问页面切片的生成，是要基于每个session的访问数据,来进行生成的。
		 * 
		 * 脱离了session生成的页面访问切片是没有意义的。
		 * 
		 * 必须是同一个人！的操作转化流
		 * 
		 * 
		 */
		
		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2actionRDD(actionRDD);
		JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();
		
		sessionid2actionsRDD = sessionid2actionsRDD.persist(StorageLevel.MEMORY_ONLY());
		
		// 最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法
		JavaPairRDD<String, Integer>  pageSplited = GenerateAndMatchPageSplit(sc, sessionid2actionsRDD, taskParam);
		Map<String, Object> pageSplitPvMap = pageSplited.countByKey();
		
		
		//使用者指定的页面流是3 2 5 8 6 
		//现在拿到的是 3-2 的pv 2-5的pv 5-3的pv 8-6的pv
		
		
		
		/**
		 * 计算起始页面的pv
		 */
		long startPagePv = getStartPagePv(taskParam, sessionid2actionsRDD);
		
	
		Map<String, Double> convertRateMap = computePageSplitConvertRate(pageSplitPvMap, startPagePv, taskParam);
		
		/**
		 * 持久化页面切片转化率
		 */
		persistConvertRate(taskid, convertRateMap);
	}
	
	/**
	 * 获取<sessionid,用户访问行为>格式的数据
	 * @param actionRDD 用户访问行为RDD
	 * @return <sessionid,用户访问行为>格式的数据
	 */
	private static JavaPairRDD<String,Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD){
		
		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String,Row>(row.getString(2),row);
			}
		});
		
	}
	
	/**
	 * 页面切片生成与匹配算法
	 * @param sc 
	 * @param sessionid2actionsRDD
	 * @param taskParam
	 * @return
	 */
	private static JavaPairRDD<String,Integer> GenerateAndMatchPageSplit(
			JavaSparkContext sc,
			JavaPairRDD<String,Iterable<Row>> sessionid2actionsRDD,
			JSONObject taskParam
			){
		
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		System.out.println("targetPageFlow"+targetPageFlow);
		final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);
		
		return sessionid2actionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {

			@Override
			public Iterable<Tuple2<String, Integer>>
			call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				List<Tuple2<String,Integer>> list = 
						new ArrayList<Tuple2<String,Integer>>();
				
				
				Iterator<Row> iterator = tuple._2.iterator();
				
				/*
				 * 获取使用者指定的页面流
				 * 使用者指定的页面流 1 2 3 4 5 6 7
				 * 1-> 2多少？  2->3 多少   
				 */
				String[] targetPages = targetPageFlowBroadcast.value().split(",");
				
				/*
				 * 这里我们拿到session的访问行为，默认情况下乱序的。
				 * 比如我们希望拿到的数据是按照实际顺序排序的
				 * 但是问题是默认是不排序的
				 * 所以第一件事情，对session的访问行为数据按照实际进行排序！！
				 */
				
				/*
				 * 举例，
				 * 比如 3-5-4-10-7
				 * 3-4-5-7-10这样排序
				 */
				List<Row> rows = new ArrayList<Row>();
				while(iterator.hasNext()){
					rows.add(iterator.next());
				}
				
				Collections.sort(rows,new Comparator<Row>() {
					@Override
					public int compare(Row o1, Row o2) {
						String actionTime1  = o1.getString(4);
						String actionTime2  = o2.getString(4);
						Date date1 = DateUtils.parseTime(actionTime1);
						Date date2 = DateUtils.parseTime(actionTime2);
						return (int)(date1.getTime()-date2.getTime());
					}
				});
				
				Long lastPageId = null;
				for(Row row:rows){
					long pageid = row.getLong(3);
					if(lastPageId == null){
						lastPageId = pageid;
						continue;
					}
					
					/*
					 * 生成一个页面切片
					 * 
					 * lastPageid = 3
					 * 5
					 * 生成切片 3_5
					 */
					String pageSplit = lastPageId +"_" +pageid;
					
					for(int i=1;i<targetPages.length;i++){
						
						//比如说,用户指定的页面流是 3 2 5 8 1
						//遍历的时候,从索引i开始 就是从第二个页面开始
						//3_2
						String targetPageSplit = targetPages[i-1]+"_"+targetPages[i];
						
						/*
						 * 当前切片正好是用户指定的！
						 */
						if(pageSplit.equals(targetPageSplit)){
							list.add(new Tuple2<String, Integer>(pageSplit, 1));
							break;
						};
					}
					
					lastPageId = pageid;
				}
				
				
				return list;
			}
		});
		
		
		
	
		
	}
	
	/**
	 * 初始页面的pv
	 *
	 * @param taskParam
	 * @param sessionid2actionsRDD
	 * @return
	 */
	private static long getStartPagePv(JSONObject taskParam,
			JavaPairRDD<String,Iterable<Row>> sessionid2actionsRDD){
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
		
		
		/*
		 * 所有访问过初始页面的session数据
		 */
		 JavaRDD<Long> startPageRDD = sessionid2actionsRDD.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				List<Long> list  = new ArrayList<Long>();
				Iterator<Row> rows = tuple._2.iterator();
				while(rows.hasNext()){
					Row row = rows.next();
					Long pageid = row.getLong(3);
					
					if(pageid == startPageId){
						list.add(pageid);
					}
				}
				return list;
			}
		});
		
		
		
		/*
		 * 计算所有访问过start page的session的个数！！
		 */
		return startPageRDD.count();
	}
	

	
	private static Map<String,Double> computePageSplitConvertRate(
			Map<String,Object> pageSplitPvMap,long startPagePv,
			JSONObject taskParam
			){
		
		String[] targetPages = 
				ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");
		
		Map<String,Double> convertRateMap =  new HashMap<String,Double>();
		
		/**
		 * 定位pv 缓存
		 */
		long lastPageSplitPv = 1L;
		
		
		/*
		 * 3 5 2 4 6
		 * 3——5
		 * 3_5 pv / 3 pv
		 * 
		 * 5_2 rate  =  5_2 pv / 3_5 pv
		 */
		
		/*
		 * 通过for循环获取目标页面流中的各个页面切片（pv）
		 */
		for(int i=1;i<targetPages.length;i++){
			String targetPageSplit = targetPages[i-1]+"_"+targetPages[i];
			
			long targetPageSplitPv = 
					Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));
		
			double convertRate = 0.0;
			if(i==1){
			//为什么第一个页面是这样计算的?
				convertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv/(double)startPagePv, 2);
			}else{
				convertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv/(double)lastPageSplitPv, 2);
			}
			convertRateMap.put(targetPageSplit, convertRate);
			lastPageSplitPv = targetPageSplitPv;
		}
		
		return convertRateMap;
		
	}
	
	
	private static void persistConvertRate(long taskid,
			Map<String,Double> convertRateMap){
		StringBuffer buffer = new StringBuffer("");
		for(Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet()) {
			String pageSplit = convertRateEntry.getKey();
			double convertRate = convertRateEntry.getValue();
			buffer.append(pageSplit + "=" + convertRate + "|");
		}
		String convertRate = buffer.toString();
		convertRate = convertRate.substring(0, convertRate.length() - 1);
		
		PageSplitConvertRate  pageSplitConvertRate = new PageSplitConvertRate();
		pageSplitConvertRate.setConvertRate(convertRate);
		pageSplitConvertRate.setTaskid(taskid);
		
		IPageSplitConvertRateDAO  PageSplitConvertRateDAO  = DAOFactory.getPageSplitConvertRateDAO();
		PageSplitConvertRateDAO.insert(pageSplitConvertRate);
		
	}
	
	
}
