package com.luoxuehuan.sparkproject.spark.session;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.luoxuehuan.sparkproject.conf.ConfigurationManager;
import com.luoxuehuan.sparkproject.constant.Constants;
import com.luoxuehuan.sparkproject.dao.ISessionAggrStatDAO;
import com.luoxuehuan.sparkproject.dao.ISessionDetailDAO;
import com.luoxuehuan.sparkproject.dao.ISessionRandomExtractDAO;
import com.luoxuehuan.sparkproject.dao.ITaskDAO;
import com.luoxuehuan.sparkproject.dao.ITop10CategoryDAO;
import com.luoxuehuan.sparkproject.dao.ITop10SessionDAO;
import com.luoxuehuan.sparkproject.dao.factory.DAOFactory;
import com.luoxuehuan.sparkproject.domain.SessionAggrStat;
import com.luoxuehuan.sparkproject.domain.SessionDetail;
import com.luoxuehuan.sparkproject.domain.SessionRandomExtract;
import com.luoxuehuan.sparkproject.domain.Task;
import com.luoxuehuan.sparkproject.domain.Top10Category;
import com.luoxuehuan.sparkproject.domain.Top10Session;
import com.luoxuehuan.sparkproject.test.MockData;
import com.luoxuehuan.sparkproject.util.DateUtils;
import com.luoxuehuan.sparkproject.util.NumberUtils;
import com.luoxuehuan.sparkproject.util.ParamUtils;
import com.luoxuehuan.sparkproject.util.StringUtils;
import com.luoxuehuan.sparkproject.util.ValidUtils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import jodd.util.StringUtil;
import scala.Tuple2;

/**
 * UserVisitSessionAnalyzeSpark
 * 
 * 用户访问session分析spark作业
 * 
 * 1.时间范围： 起始日日期-结束日期
 * 2.性别：男 女
 * 3.年龄范围
 * 4.职业 多选
 * 5.城市 多选
 * 6.搜索词 多个搜索词 只要某个session中的任何一个action搜索过指定的关键词,那么session就符合条件
 * 7.点击品类： 多个品类，只要某个session中任何一个action点击过某个品类，那么session就符合条件
 * 
 * 
 * 我们的spark作业如何接受用户创建的任务？
 * 
 * J2EE平台接受创建任务请求，将信息插入Mysql，任务参数以JSON格式封装。
 * J2EE平台执行spark-submit shell脚本 并将taskid 并将taskid 作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接受参数的，并且会将接收到的参数，传递给spark作业的main函数
 * 函数就封装在main函数的args数组中
 * 
 * 这是spark本身提供的特性
 * 
 * @author lxh
 * 
 * {"startAge":["10"],"endAge":["50"],"startDate":["2015-08-01"],"endDate":["2016-08-03"]}
 *
 */
public class UserVisitSessionAnalyzeSpark {

	public static void main(String[] args) {

		/*
		 * 构建spark上下文
		 */
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
				.setMaster("local")
				.set("spark.storage.memoryFraction","0.5") 	//降低cache操作的内存占比 默认为0.6
				.set("spark.shuffle.consolidateFiles", "true")  //是否开启shuffle端map输出文件合并，默认为false
				.set("spark.shuffle.file.buffer", "64")  //map端内存缓存区大小，默认为32k 调成64
				.set("spark.shuffle.memoryFraction", "0.3") //shuffle,reduce端 内存占比，默认为0.2 调成0.3
				.set("spark.reducer.maxSizeInFlight","24")// 调小reduce端内存缓冲  减少内存占用 而OOM【troubleshooting】
				.set("spark.shuffle.io.maxRetries","60")//拉去shuffle文件失败 重试次数 60次【troubleshooting】
				.set("spark.shuffle.io.retryWait","60s")//拉取shuffle文件失败后等待时长 60s【troubleshooting】
				.set("spark.serializer","org.apache.spark.serializer.KryoSerializer") //序列化方式,默认为Java的。如果调成Kryo必须注册类。
				.registerKryoClasses(new Class[]{
						CategorySortKey.class,
						IntList.class});
		
		/**
		 * 实际调优肯定要根据实际情况调优的！！！
		 */
		
		/**
		 * 比如获取top10热门品类功能中，二次排序，自定义了一个key
		 * 那个key是需要进行shuffle 的时候，进行网络传输的，因此也是要求实现序列化的
		 * 启用kryo机制以后，就会用kryo去序列化和反序列化。
		 * 所以这里要求，注册这个类。
		 */

		JavaSparkContext sc = new JavaSparkContext(conf);
		//sc.checkpointFile("hdfs:/");//设置checkpoint目录
		SQLContext sqlContext = getSQLContext(sc.sc());

		// 生产模拟测试数据
		mockData(sc, sqlContext);
		/**
		 * 创建需要使用的DAO组件
		 */
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		
		
		/*
		 * 如果要进行session粒度的数据聚合
		 * 首先要从user_visit_action表中，查询出来制定日期范围内的行为数据。
		 * 如果要根据用户在创建任务时指定的参数，来进行数据过滤和筛选
		 * 那么就首先得查询出来制定的任务
		 */
		args = new String[]{"2"};
		long taskid = 2L;//ParamUtils.getTaskIdFromArgs(args, "1");
		Task task =  taskDAO.findById(taskid);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		
		/**
		 * actionRDD就是一个公共RDD。
		 * 第一，要用actionRDD，获取到一个公共的sessionid位key的pairRDD
		 * 第二个，actionRDD，用在了session 聚合环节里面。
		 * 
		 * sessionid位key的PairRDD，是确定了，在后面要多次使用的。
		 * 1.与通过筛选的sessionid进行join,获取通过筛选的session明细数据。
		 * 2.将这个RDD，直接传入aggregateBySession,进行session聚合统计。
		 * 
		 * 重构完以后,actionRDD,就只有在最开始的时候，使用一次,用来生成以sessionid位key的RDD
		 */
		/*
		 * 这里是拿到【模拟数据】。一个session里面的每一个action都 都带着一个action信息。
		 * 现在把这些action信息聚合到一个session里面。
		 */
		JavaRDD<Row> actionRDD = getActionRDDByDataRange(sqlContext, taskParam);
		System.out.println("actionRDD条数——————————————————————————————————————————————————————————————————————————————————————————————————————————"+actionRDD.count());
		
		
		/**
		 * sessionid2ActionRDD在后面使用了2次？是否要【重构RDD2】
		 *  
		 *  要进行持久化！！
		 *  
		 * TODO
		 */
		JavaPairRDD<String, Row>  sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);
		
		/**
		 * 进行持久化
		 * 
		 * 
		 * 如果是persist(StorageLevel.MEMORY_ONLY()); 纯内存，无序列号，那么可以用cache()方法来替代
		 * 
		 * StorageLevel.MEMORY_ONLY_SER() ser序列化
		 * 
		 * StorageLevel.MEMORY_AND_DISK()
		 * 
		 * StorageLevel.MEMORY_AND_DISK_SER()
		 * 
		 * StorageLevel.MEMORY_AND_DISK_SER_2()
		 * 
		 * 
		 */
		sessionid2ActionRDD = sessionid2ActionRDD.persist(StorageLevel.MEMORY_ONLY());
		//sessionid2ActionRDD.checkpoint();//cache后，进一步checkpoint，如果cache过，直接拿，如果没有cache过，重新算一遍。
		
		/**
		 * 思路分析：
		 * 怎么来聚合？
		 * 
		 * 首先可以将行为数据 按照session——id 进行groupbykey分组。
		 * 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
		 * 与用户信息的数据进行join
		 * 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
		 * 到这里为止获取的数据是<sessionid,(sessionid,serchkeywords,clickcategoryids,age,professional,city,sex)>
		 */
		
		/*
		 * 这里获得的是【session和用户信息聚合的数据】。
		 * 数据是<sessionid,(sessionid,serchkeywords,clickcategoryids,age,professional,city,sex)>
		 */
		//JavaPairRDD<String, String>  sessionid2AggrInfoRDD = aggregateBySession(actionRDD, sqlContext);
		
		/**
		 * 因为，已经对actionRDD计算过一次了。JavaPairRDD<String, Row>  sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);
		 * 进行【重构RDD1】,actionRDD在这个方法里面,又进行了这个逻辑的计算,所以把 计算过的sessionid2AggrInfoRDD 直接当做参数传入这个方法。
		 */
		JavaPairRDD<String, String>  sessionid2AggrInfoRDD = aggregateBySession(sc,sessionid2ActionRDD, sqlContext);
		
		/*System.out.println("sessionid2AggrInfoRDD条数——————————————————————————————————————————————————————————————————————————————————————————————————————————"+sessionid2AggrInfoRDD.count());
		for(Tuple2<String,String> tuple : sessionid2AggrInfoRDD.take(10)){
			System.out.println("每条数据——"+tuple._2);
		}*/
		
		
		
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
				"",new SessionAggrAccumulator());
		/**
		 * 接着就用针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
		 * 相当于我们自己编写的算子，是要访问外面的任务参数对象的。
		 * 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象
		 * 是要给外部对象使用final修饰的。
		 */
		JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
				sessionid2AggrInfoRDD, taskParam,sessionAggrStatAccumulator);
		
		/**
		 * 【重构RDD3】
		 * 点击这filteredSessionid2AggrInfoRDD 看代码，可以看到它被使用了2次！！！
		 * 所以也要进行持久化
		 */
		filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
		
		/**
		 * 生成公共的RDD：通过筛选条件的session明细数据
		 * 
		 * 【重构RDD4】
		 */
		JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);
		
		/**
		 * sessionid2detailRDD 在之后被使用了3次，需要进行[持久化]。
		 */
		sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());
		
		/**
		 * TODO
		 * 对于accumulator这种分布式累加计算的变量的使用，有一个重要说明
		 * 
		 * 从accumulatro中，获取数据，插入数据库的时候，
		 * 
		 * 如果没有action的话，那么整个程序根本不会运行。
		 * 
		 * 是不是在calculateAndPersistAggrStat方法之后运行一个action操作，比如count，take
		 * 不对！
		 * 
		 * @@@@@@@必须把能够触发job执行的操作，放在最终写入MySQL方法之前！！！
		 * 
		 * 计算出来的结果是用柱状图显示的！
		 */
		System.out.println(filteredSessionid2AggrInfoRDD.count());//触发一下job，如果有因为随机抽取功能中，有个countByKey算子，是action操作，会触发job。就不需要这个count。
		
		
		/**
		 * 随机抽取session！
		 */
		
		randomExtractSession(sc,task.getTaskid(),sessionid2AggrInfoRDD,sessionid2detailRDD);
		
		
		/**
		 * 特别说明。
		 * 我们 知道要讲上 一个功能的session聚合统计数据获取到，就必须是在一个action操作触发后。
		 * 才能从accumulator中获取数据。
		 * 
		 * 所以，我们在这里将随机抽取的功能中，将随机抽取的功能的实现代码，放在session聚合统计功能的最终计算和写库之前
		 * 因为随机抽取功能中，有个countByKey算子，是action操作，会触发job。
		 */
		//计算出各个范围的session占比，并写入MySQL【session聚合统计写库】
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),task.getTaskid());
		
		/**
		 * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
		 * 
		 * 1.actionRDD,映射成<sessionid,Row>的格式
		 * 2.按Sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
		 * 3.遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中对应的值。
		 * 4.使用自定义的Accumulator的统计值，去计算各个区间的比例。
		 * 5.将最后计算出来的结果，写入MySQL对应的表中。
		 * 
		 * 
		 * 普通实现思路的问题：
		 * 1.为什么要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了，多次一举。
		 * 2.是不是一定要为了session的聚合这个功能，单独去遍历一遍session？ 其实没有必要，一举有
		 *  之前过滤session的时候，其实就相当于 在遍历session，那么这里就没有必要再过滤一遍。
		 *  
		 *  重构实现思路(但也有可能导致耦合性降低? )
		 *  
		 *  1,不要去生成任何新的RDD（可能要处理商议的数据）
		 *  2.不要去单独遍历一遍session的数据（处理上千万的数据）
		 *  3.可以在进行session聚合的时候，就直接计算出来每个session的访问步长和时长。
		 *  4. 在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后，
		 *  	将其访问时长和访问步长，累加到自定义的Accumulator上面去。
		 *  5. 就是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达半个小时，
		 *  	或者数个小时。
		 * 
		 * 
		 * 
		 * 开发Spark大型复杂项目的一些经验准则：
		 * 
		 * 1.尽量少生成RDD
		 * 2.尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能！
		 * 3.尽量少对RDD进行shuffle 算子操作。比如groupByKey，reduceByKey，sortByKey（而 用普通的map，mapToPair替代）
		 * 		shuffle操作，会导致大量的磁盘读写，严重降低性能。
		 * 		有shuffle 的算子和没有shuffle的算子，甚至性能会达到几十分钟，甚至数个小时的差别。
		 * 		有shuffle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手。
		 * 4，无论做什么功能，性能第一
		 * 		在传统的J2EE或者.NET或者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要程度，远远高于了性能，大量的分布式的架构，设计模式
		 * 		代码的划分，类的划分（高并发网站除外）
		 * 	
		 * 		在大数据项目中，比如MaprReduce，Hive Spark Storm我认为性能是重要程度，远远大于一些代码的规范，和设计模式，代码的划分，类的划分，
		 * 			大数据，大数据最重要的，就是性能！！性能！！性能！！
		 * 		主要是因为大数据以及大数据项目的特点，决定了，大数据程序和项目的速度，都比较慢。
		 * 		如果不优先考虑性能的话，会导致大数据 处理程序运行时间长度数个小时，甚至数十个小时。（Hive写的sql语句可能会跑很长时间）
		 * 		此时对于用户体验，简直就是一场灾难。 
		 * 
		 * 		所以推荐，大数据项目中，在开发和代码的架构中，优先考虑性能，其次考虑功能代码的划分，解耦合。
		 * 
		 */
		
		
		/**
		 * 抽取三步：
		 * 1.计算出每天每小时session数量
		 * 2.按时间比例抽取的算法
		 * 3.根据算法 计算出来的索引，根据索引 抽取，并写入mysql
		 * 4.测试。
		 */
		
		
		/*
		 * 因为有了sessionid2detailRDD,所以就不用两个rdd参数了
		 * 为什么要抽取出来，因为getTop10Session也要用到！！！
		 */
		
		/**
		 * 获取排名前10的热门品类
		 */
		//getTop10Category(task.getTaskid(),filteredSessionid2AggrInfoRDD,sessionid2ActionRDD);
		List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(task.getTaskid(),sessionid2detailRDD);
		
		
		getTop10Session(sc,task.getTaskid(),top10CategoryList,sessionid2detailRDD);
		/*while(true){
			
		}*/
		/*sc.close();*/

	}




	/**
	 * 获取top10活跃session
	 * 
	 * 什么是top10活跃session?
	 * 	
	 * @param taskid
	 * @param sessionid2detailRDD
	 */
	private static void getTop10Session(	JavaSparkContext sc ,final long taskid,
			List<Tuple2<CategorySortKey, String>> top10CategoryList,
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		/**
		 * 第一步：将top10热门品类的id,生成一份RDD
		 */
		
		List<Tuple2<Long, Long>> top10CategoryIdList 
		= new ArrayList<Tuple2<Long, Long>>();
		
		for(Tuple2<CategorySortKey, String> category : top10CategoryList){
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
					category._2, "\\|", Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));
		}
		
		/*
		 * 
		 * 想要将list变成rdd 必须需要sc
		 * 
		 * 生成这10个品类组成的RDD,以便后面的join
		 */
		 JavaPairRDD<Long, Long>  top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);
		
		 
		 /**
		  * 第二步：计算top10 品类被各session点击的次数!
		  */
		 
		 /*
		  * 为什么要groupByKey
		  */
		 JavaPairRDD<String, Iterable<Row>>  sessionid2detailsRDD =  sessionid2detailRDD.groupByKey();
		 
		 
		 /*
		  * 格式是 <categoryid,sessionid:count>
		  * 因为是flatMapToPair 所以是迭代flat处理过的！(1,2,3),3的flat就是 1,3 和 2,3 和 3,3
		  */
		 JavaPairRDD<Long, String>  categoryid2sessionCountRDD   = sessionid2detailsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				String sessionid = tuple._1;
				Iterator<Row> iterator = tuple._2.iterator();
				Map<Long,Long> categoryCountMap = new HashMap<Long,Long>();
				
				
				/**
				 * 计算出该session对每个品类的点击次数!
				 */
				while(iterator.hasNext()){
					Row row = iterator.next();
					
					if(row.get(6) !=null){
						long categoryid = row.getLong(6);
						Long count  = categoryCountMap.get(categoryid);
						if(count ==null ){
							count = 0L;
						}
						count++;
						categoryCountMap.put(categoryid,count);
		 			}
				}
				
				
				/*
				 * 返回结果 <categoryid,sessionid:count>格式
				 * 
				 * 组装格式！
				 */
				List<Tuple2<Long,String>> list  = new ArrayList<Tuple2<Long,String>>();
				for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()){
					
					long categoryid = categoryCountEntry.getKey();
					long count  = categoryCountEntry.getValue();
					String value = sessionid + ","+count;
					list.add(new Tuple2<Long, String>(categoryid,value));
				}
				
				
				
				return list;
			}
		});
		 
		 
		 /*
		  * 拿到10个热门品类被各个session点击的次数
		  */
		 JavaPairRDD<Long, String>  top10CategorySessionRDD
		 =  top10CategoryIdRDD.join(categoryid2sessionCountRDD).mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {

	
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long,String>(tuple._1,tuple._2._2);
			} 
		});
		 
		 /**
		  * 第三步,取topN的算法！
		  */
		 
		 JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =  
				 top10CategorySessionRDD.groupByKey();
		 
		 /*
		  * 要拿到什么信息。（品类，品类被各个session的点击次数）
		  */
		 JavaPairRDD<String, String> top10SessionIdRDD = top10CategorySessionCountsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
				long categoryid = tuple._1;
				Iterator<String> iterator = tuple._2.iterator();
				
				/**
				 * BUG
				 * 有些品类根本没有top10 改成3 根本没有10个session!!!
				 * 
				 * 
				 * 对  某一个品类 点击次数 排名前十的session
				 * 例如品类位 67  对品类67 点击前十的分别是 A 45 B 44 ..这样！！！
				 */
				//定义取排序的top10数组
				String[] top10Sessions = new String[10];
				
				while(iterator.hasNext()){
					
					String sessionCount = iterator.next();
					String sessionid = sessionCount.split(",")[0];
					long count = Long.valueOf(sessionCount.split(",")[1]);
					
					//遍历排序数组
					for(int i = 0 ;i<top10Sessions.length;i++){
						
						//如果当前i位没有数据，那么直接将i位数据赋值为当前sessionCount
						if(top10Sessions[i] == null	){
							top10Sessions[i] = sessionCount;
							break;
						}else{
							long _count = Long.valueOf(sessionCount.split(",")[1]);
							//如果sessionCount比i位的sessionCount要大
							
							if(count > _count){
								//从排序数组最后一位开始，到i位，所有数据往后挪一位
								for(int j=9;j>i;j++){
									top10Sessions[j] = top10Sessions[j-1];
								}
								
								//将i位赋值为sessionCount
								top10Sessions[i]  = sessionCount;
								break;
							}
							
							//比较小,继续外层for循环
						}
					}
				}
				
				
				List<Tuple2<String,String>> list   = new ArrayList<Tuple2<String,String>>();
				
				for(String sessionCount : top10Sessions){
					
					/**
					 * BUG
					 * 有些品类根本没有top10 改成3 根本没有10个session!!!
					 * 所以要做非空判断!
					 */
					if(sessionCount!=null){
						String sessionid = sessionCount.split(",")[0];
						long count  =Long.valueOf(sessionCount.split(",")[1]);
						
						/*
						 * 将top10Session插入MySQL表
						 */
						Top10Session top10Session = new Top10Session(taskid,categoryid,sessionid,count);
						
						ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
						top10SessionDAO.insert(top10Session);
						
						/*
						 * 放入list
						 */
						list.add(new Tuple2<String,String>(sessionid,sessionid));
					}
				}
				
				/**
				 * 为什么是返回list
				 */
				return list;
			}
		});
		 
		 /**
		  * 第四步 ：获取top10session的明细 数据并存入MySQL（SessionDetail）
		  * TODO
		  */
		 JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
				 top10SessionIdRDD.join(sessionid2detailRDD); 
		 
		 sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				//String sessionid = tuple._1;	
				Row row  = tuple._2._2;
				
				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskid);  
				sessionDetail.setUserid(row.getLong(1));  
				sessionDetail.setSessionid(row.getString(2));  
				sessionDetail.setPageid(row.getLong(3));  
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));  
				sessionDetail.setClickCategoryId(row.getLong(6));  
				sessionDetail.setClickProductId(row.getLong(7));   
				sessionDetail.setOrderCategoryIds(row.getString(8));  
				sessionDetail.setOrderProductIds(row.getString(9));  
				sessionDetail.setPayCategoryIds(row.getString(10)); 
				sessionDetail.setPayProductIds(row.getString(11));  
				
				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insert(sessionDetail);
				
			}
		});
		 
		 
		 
		 
		 
	}




	/**
	 * 【算子优化1 map 改成mapPartitions】
	 * @param actionRDD
	 * @return
	 */
	public static JavaPairRDD<String,Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
		/*return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String,Row>(row.getString(2),row);
			}
		});*/
		
		return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
				List<Tuple2<String,Row>> list  = new ArrayList<Tuple2<String,Row>>();
				while(iterator.hasNext()){
					Row row =iterator.next();
					list.add(new Tuple2<String,Row>(row.getString(2),row));
				}
				return list;//返回一个迭代器，代表多条数据！！！
			}
		});
		
	}


	/**
	 * 抽取出来成为公共的RDD、
	 * 获取通过筛选条件的session的访问明细数据RDD
	 * @param sessionid2aggrInfoRDD
	 * @param sessionid2actionRDD
	 * @return
	 */
	private static JavaPairRDD<String,Row> getSessionid2detailRDD(
			JavaPairRDD<String,String> sessionid2aggrInfoRDD,
			JavaPairRDD<String,Row> sessionid2actionRDD){
		
		JavaPairRDD<String, Row>  sessionid2detailRDD  = sessionid2aggrInfoRDD
				.join(sessionid2actionRDD)
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {


					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String,  Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
						/*
						 * 放回了sessionid和action 去掉了session明细。
						 */
						return new Tuple2<String,Row>(tuple._1,tuple._2._2);
					}
			});
		
		return sessionid2detailRDD;
		
	}
	
	private static void randomExtractSession(
			JavaSparkContext sc,
			final long taskid,
			JavaPairRDD<String, String> sessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2detailRDD ) {
	
		/**
		 * 第一步,计算出每天每小时的session数量，获取<yyyy-MM-dd_HH ,sessionid>格式的rdd
		 */
		JavaPairRDD<String, String>  time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
				String aggrInfo = tuple._2;
				String start_time = StringUtils.getFieldFromConcatString(
						aggrInfo, "\\|", Constants.FIELD_START_TIME);
				String dateHour = DateUtils.getDateHour(start_time);//yyyy-MM-dd_HH
				//return new Tuple2<String,String>(dateHour,sessionid);
				return new Tuple2<String,String>(dateHour,aggrInfo);
			}
		});
		
		
		
		
		/**
		 * 思考一下,我们这里不要着急写大量代码,做项目的时候，一定要用脑子多思考。
		 * Tuple2<String,String>(dateHour,sessionid);
		 * 改写成：
		 * Tuple2<String,String>(dateHour,aggrInfo);
		 */
		
		/**
		 * 每天每小时的session 数量的计算
		 * 是有可能出现数据倾斜的把。这个是没有疑问的。
		 * 
		 * 比如大部分小时一般访问量也就10万，但是呢，中午12点的时候，高峰期，一个小时 一千万！！！
		 * 
		 * 这个时候，就会发生数据倾斜了！
		 * 
		 * 我们就用这个countByKey操作 第三种和第四种(双重group)
		 * 
		 * ERROR!!
		 * 
		 * 是一个action操作，不能 用分区！！10!
		 */
		//得到每天每小时的session数量
		Map<String ,Object> countMap = time2sessionidRDD.countByKey();
		//Map<String ,Object> countMap = time2sessionidRDD.countByKey();
		
		
		System.out.println("第一步每天每小时session数量："+time2sessionidRDD.count());
		
		/**
		 * 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引【***获得索引***】
		 */
		
		//将<yyyy-MM-DD_HH,count>格式的map,转换成<yyyy-MM-dd,<HH,count>>的格式
		Map<String,Map<String,Long>> dateHourCountMap = new HashMap<String,Map<String,Long>>();
		
		for(Map.Entry<String, Object> countEntry:countMap.entrySet()){
			String dateHour = countEntry.getKey();
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			
			long count = Long.valueOf(String.valueOf(countEntry.getValue()));
			System.out.println("date ----hour"+date+hour+"count"+count);
			Map<String,Long> hourCountMap = dateHourCountMap.get(date);
			if(hourCountMap == null){
				hourCountMap = new HashMap<String,Long>();
				dateHourCountMap.put(date, hourCountMap);
			}
			hourCountMap.put(hour, count);
		}
		
		System.out.println("dateHourCountMap.size():"+dateHourCountMap.size());
	
		
		//开始实现我们按时间比例随机抽取算法
		
		//总共要抽取100个session,先按照天数,进行平分
		
		int extractNumberPerDay = 100 / dateHourCountMap.size();
		
		
		System.out.println("extractNumberPerDay"+extractNumberPerDay);
		
		
		/**
		 * 用到了一个比较大的变量，随机抽取索引map
		 * 之前是直接在算子里面使用了这个amp,那么根据我们刚才讲的这个原理,每个task都会拷贝一份map副本 
		 * 还是比较消耗内存和网络传输性能的。
		 * 
		 * 将map做成广播变量。
		 */
		
		//<date,<hour,(3,5,20,123[索引list])>>
		Map<String, Map<String, List<Integer>>> dateHourExtractMap = 
				new HashMap<String, Map<String, List<Integer>>>();
		Random random = new Random();
		
		for(Map.Entry<String, Map<String,Long>> dateHourCountEntry : dateHourCountMap.entrySet()){
			String date  = dateHourCountEntry.getKey();
			Map<String,Long> hourCountMap = dateHourCountEntry.getValue();
			
			
			//计算出这一天的session总数
			long sessionCount = 0L;
			
			for(long hourCount:hourCountMap.values()){
				sessionCount+=hourCount;
			}
			
			//遍历每个小时
			for(Map.Entry<String,Long> hourCountEntry:hourCountMap.entrySet()){
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();
				
				//计算【每个小时的session数量】,占据每天总session数量的【比例】,直接乘以【每天要抽取的数量】
				//就可以计算出,当前小时要抽取的session数量。
				int hourExtractNumber = (int) (((double)count/(double)sessionCount) 
						* extractNumberPerDay);
			
				System.out.println("当前小时要抽取的session数量。hourExtractNumber"+hourExtractNumber);
				
				//如果这个session总session很少。
				if(hourExtractNumber > count){
					hourExtractNumber = (int) count;
				}
				Map<String,List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
				
				if(hourExtractMap==null){
					hourExtractMap = new HashMap<String,List<Integer>>();
					dateHourExtractMap.put(date, hourExtractMap);
				}
				 
				
				List<Integer>  extractIndexList = hourExtractMap.get(hour);
				if(extractIndexList==null){
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}
				
				//生成上面计算出来的数量的随机数
		/*		for(int i = 0;i<extractNumberPerDay;i++){
					int extractIndex = random.nextInt((int)count);
					System.out.println("要抽取的索引的下标i："+i);
					//不能重复
					while(extractIndexList.contains(extractIndex)){
						extractIndex = random.nextInt((int)count);
						System.out.println("要抽取的索引的下标extractNumberPerDay："+extractNumberPerDay);
						System.out.println("要抽取的索引为extractIndex："+extractIndex);
					}
					
					extractIndexList.add(extractIndex);
					
				}*/
				
				
				// 生成上面计算出来的数量的随机数
				for(int i = 0; i < hourExtractNumber; i++) {
					int extractIndex = random.nextInt((int) count);
					while(extractIndexList.contains(extractIndex)) {
						extractIndex = random.nextInt((int) count);
						System.out.println("要抽取的索引为extractIndex："+extractIndex);
					}
					extractIndexList.add(extractIndex);
				}
			}
			
		}
		
		/**
		 * fastutil的使用很简单,比如List<Integer> 的list 对应到fastutil，就是IntList
		 */
		Map<String, Map<String, IntList>> fastutilDateHourExtractMap  = 
				new HashMap<String, Map<String, IntList>>();
		
		for(Map.Entry<String, Map<String,List<Integer>>> dateHourExtractentry:
			dateHourExtractMap.entrySet()){
			
			String date = dateHourExtractentry.getKey();
			Map<String,List<Integer>> hourExtractMap = dateHourExtractentry.getValue();
			Map<String,IntList> fastutilHourExtractMap = new HashMap<String,IntList>();
			
			for(Map.Entry<String, List<Integer>> hourExtractentry:hourExtractMap.entrySet()){
				String hour = hourExtractentry.getKey();
				List<Integer> entractList = hourExtractentry.getValue();
				
				IntList fastutilExtractList = new IntArrayList();
				for(int i=0;i<entractList.size();i++){
					fastutilExtractList.add(entractList.get(i));
				}
				
				fastutilHourExtractMap.put(hour, fastutilExtractList);
				
			}
			
			fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
			
		}
		
		/**
		 * 广播变量，很简单。
		 * 其实就是在sc上设置一个broadCast
		 */
		//final Broadcast<Map<String, Map<String, List<Integer>>>> dateHourExtractMapBroadCast= sc.broadcast(dateHourExtractMap);
		
		final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadCast= sc.broadcast(fastutilDateHourExtractMap);
		
		
		/**
		 * 第三步
		 * 
		 * 遍历每天每小时的session,然后根据随机索引进行抽取
		 */
		
		JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();
	
		/*
		 * 我们用flatMap算子,遍历所有的<dateHour,(session aggrInfo)>格式的数据
		 * 
		 * 然后呢,会遍历每天每小时的session
		 * 
		 * 如果发现某个session恰巧在我们指定的这个小时的随机索引上
		 * 那么抽取该session,直接写入MySQL的random_extract_session表
		 * 将抽取出来的session_id返回回来，形成一个新的JavaRDD<String>
		 * 
		 * 然后最后一步,是用抽取出来的sessionid,去join他们的访问行为明细数据,写入session表。
		 * 
		 */
	
		/**
		 * 重新思考,为了之后要join【访问明细数据】,把flatmap变成flatmaptopair
		 */
		JavaPairRDD<String,String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, String>> call(
					Tuple2<String, Iterable<String>> tuple) throws Exception {

				List<Tuple2<String, String>> extractSessionids = 
						new ArrayList<Tuple2<String, String>>();
				
				String dateHour = tuple._1;
				String date = dateHour.split("_")[0];
				String hour = dateHour.split("_")[1];
				
				Iterator<String> iterator = tuple._2.iterator();
				
				/**
				 * 要使用广播变量的时候，就直接从broadcast中getValue
				 */
				//Map<String, Map<String, List<Integer>>> dateHourExtractMap = dateHourExtractMapBroadCast.getValue();
				/**
				 * 改用fastutil 。 
				 * 
				 * 其他地方，不用改！因为实现了list接口！！！
				 * 
				 * 但是要在kryo里面注册！！
				 */
				Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadCast.getValue();
				
				
				
				//拿到索引
				List<Integer> extractIndeList = dateHourExtractMap.get(date).get(hour);
				ISessionRandomExtractDAO  sessionRandomExtractDAO  = DAOFactory.getSessionRandomExtractDAO();
				
				int index = 0 ;
				while(iterator.hasNext()){
					String sessionAggrInfo = iterator.next();
					if(extractIndeList.contains(index)){
						
						String sessionid = StringUtils.getFieldFromConcatString(
								sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						SessionRandomExtract  sessionRandomExtract = new SessionRandomExtract();
						sessionRandomExtract.setTaskid(taskid);
						sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
								sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));  
						sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
								sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
						sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
								sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
						
						sessionRandomExtractDAO.insert(sessionRandomExtract);  
						
						// 将sessionid加入list
						extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid)); 
					
					}
					index++;
				}
				return extractSessionids;
			}
		});
		
		
		/**
		 * 第四步： 获取抽取出来的session的明细数据
		 * 
		 * BUG 改正,这里之前传错了,传承了sessionid2ActionRDD
		 */
		JavaPairRDD<String, Tuple2<String, Row>>  extractSessionDetailRDD = 
				extractSessionidsRDD.join(sessionid2detailRDD);
		
		
		/*extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {
			
			@Override
			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				Row row = tuple._2._2;
				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskid);  
				sessionDetail.setUserid(row.getLong(1));  //从1开始，不是从0开始！
				sessionDetail.setSessionid(row.getString(2));  
				sessionDetail.setPageid(row.getLong(3));  
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));  
				sessionDetail.setClickCategoryId(row.getLong(6));  
				sessionDetail.setClickProductId(row.getLong(7));   
				sessionDetail.setOrderCategoryIds(row.getString(8));  
				sessionDetail.setOrderProductIds(row.getString(9));  
				sessionDetail.setPayCategoryIds(row.getString(10)); 
				sessionDetail.setPayProductIds(row.getString(11));  
				
				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insert(sessionDetail);  
				
			}
		});*/
		
		/**
		 * 【算子优化3】【foreach改为foreachPartition】
		 */
		extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>() {

			@Override
			public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
				
				List<SessionDetail> sessionDetails= new ArrayList<SessionDetail>();
				while(iterator.hasNext()){
					/*Tuple2 tuple = iterator.next();
					Tuple2 obj = (Tuple2 )tuple._2;
					Row row = (Row)obj._2;*/
					
					/**
					 * 这里应该直接标注类型！！！
					 */
					Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();
					Row row = tuple._2._2;
					
					SessionDetail sessionDetail = new SessionDetail();
					sessionDetail.setTaskid(taskid);  
					sessionDetail.setUserid(row.getLong(1));  //从1开始，不是从0开始！
					sessionDetail.setSessionid(row.getString(2));  
					sessionDetail.setPageid(row.getLong(3));  
					sessionDetail.setActionTime(row.getString(4));
					sessionDetail.setSearchKeyword(row.getString(5));  
					sessionDetail.setClickCategoryId(row.getLong(6));  
					sessionDetail.setClickProductId(row.getLong(7));   
					sessionDetail.setOrderCategoryIds(row.getString(8));  
					sessionDetail.setOrderProductIds(row.getString(9));  
					sessionDetail.setPayCategoryIds(row.getString(10)); 
					sessionDetail.setPayProductIds(row.getString(11));  
					sessionDetails.add(sessionDetail);
					
				}
				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insertBatch(sessionDetails);
				
			}
		});
		
		
		
	}


	/**
	 * 如果是生产环境用hiveContext 如果是测试环境用sqlContext
	 * 
	 * @param sc
	 * @return
	 */
	private static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			return new SQLContext(sc);

		} else {
			return new HiveContext(sc);
		}

	}

	private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			MockData.mock(sc, sqlContext);

		}
	}
	
	private static JavaRDD<Row> getActionRDDByDataRange(
			SQLContext sqlContext,JSONObject taskParam){
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		
		String sql = "select * "
				+ "from user_visit_action "
				+ "where date>='"+startDate +"' "
				+ "and date<='"+endDate + "' ";
//【数据倾斜2】				+ "and session_id not in('','','')"	
		
		/**
		 * 进行一步时间过滤
		 */
		DataFrame actionDF = sqlContext.sql(sql);
		
		
		/**
		 *  【算子调优repatition】
		 *  
		 *  这里就很有可能查出来的rdd 并行度很低。
		 *  比如说，spark sql默认就给第一个stage设置了20个task，但是根据你的数据量和算法的复杂度
		 *  实际上，你需要1000个task去并行执行。
		 *  
		 *  所以说，在这里，就可以对spark sql刚刚查出来的rdd执行repartition重分区操作。
		 */
		
		//return actionDF.javaRDD().repartition(1000);//测试环境local不需要 已经优化过，先注释掉！
	
		return actionDF.javaRDD();
	}

	
	private static JavaPairRDD<String,String> aggregateBySession(
			JavaSparkContext sc,
			JavaPairRDD<String,Row> sessionid2ActionRDD,
			SQLContext sqlContext){
		//现在actionRDD中的元素是Row,一个Row就是一行用户访问行为记录，比如一次点击或者搜索
		//我们需要将这个Row映射成<Sessionid,Row>的格式
		
		
		/**
		 * 第一个参数：相当于函数的输入
		 * 第二个和第三个 输出
		 */
		/**
		 * 【重构RDD1】后，这段就不要了！因为已经计算过了！！
		 */
	/*	JavaPairRDD<String ,Row> sessionid2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				
				return new Tuple2<String ,Row>(row.getString(2),row);
			}
		});*/
		
		
		//位行为数据按session粒度进行分组
		JavaPairRDD<String ,Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();
		System.out.println("sessionid2ActionsRDD1:"+sessionid2ActionsRDD.count());
		
		
		/**
		 * 算子优化 reduceByKey
		 * 怎么改用reduceByKey  (_ + _)  改成? (tuple.key + tuple.value)?????猜的
		 * 
		 * 
		 */
		
		//对每一个session分组进行聚合,将session中所有的搜索词和点击品类都聚合起来
		JavaPairRDD<Long, String>  userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

			/**
			 * 28.36
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				
				
				String sessionid = tuple._1;
				Iterator<Row> iterator = tuple._2.iterator();
				StringBuffer serachKeywordsBuffer = new StringBuffer("");
				StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
				
				Long userid = null;
				 
				Date startTime = null;
				Date endTime = null;
				
				//访问步长【】
				int stepLength = 0;
				
				
				/*
				 * 遍历session所有的访问行为
				 */
				while(iterator.hasNext()){
					Row row  = iterator.next();
					
					if(userid == null){
						userid = row.getLong(1);
						//System.out.println("userid:==============="+userid);
					}
					String searchKeyword  = row.getString(5);
					Long clickCategoryId = row.getLong(6);
					/*
					 * 实际上这里要对数据说明一袭
					 * 并不是每一行访问行为都有searchKeyword和clickcategoryid两个字段的
					 * 其实只有搜索行为，和点击行为有相应字段。
					 * 所以任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
					 * 
					 * 我们决定是否将搜索词或者点击品类id拼接到字符串中去
					 * 首先要满足，不能是null值
					 *  其次，之前的字符串中还没有搜索词或者点击品类id
					 */
					
					if(StringUtil.isNotEmpty(searchKeyword)){
						if(!serachKeywordsBuffer.toString().contains(searchKeyword)){
							serachKeywordsBuffer.append(searchKeyword+",");
						}
					}
					
					if(clickCategoryId != null){
						if(!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
							clickCategoryIdsBuffer.append(clickCategoryId+",");
						}
					}
					
					
					//计算开始时间和结束时间
					Date actionTime = DateUtils.parseTime(row.getString(4));
					if(startTime == null){
						startTime = actionTime;
					}
					if(endTime == null){
						endTime = actionTime;
					}
					if(actionTime.before(startTime)){
						startTime = actionTime;
					}
					if(actionTime.after(endTime)){
						endTime = actionTime;
					}
					
					//计算session访问步长【步长？几步操作。点击几次】
					
					stepLength++;
					
					
				}
				
				String searchKeyWords = StringUtils.trimComma(serachKeywordsBuffer.toString()) ;
				String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
				
				//计算session访问时长（结束时间-开始时间）（秒,所以/1000）
				long visitLength = (endTime.getTime() - startTime.getTime())/1000;
				
				/**
				 * 我们返回的数据格式,即是<sessionid,partAggrInfo>
				 * 但是我们这一步聚合完以后，其实我们还需要将每一行数据，跟对应的用户信息进行聚合
				 * 问题就来了，如果是跟用户进行聚合的话，那么key，就不应该是sessionid
				 * 就应该是userid，才能够和<userid,Row>格式的用户信息就行聚合
				 * 如果我们这里直接返回<sessionid,partAggrInfo>,还得再做一次mapToPair算子
				 * 
				 * 将RDD映射成<userid,partAggrInfo>的格式，那么就多次一句
				 * 
				 * 所以我们这里其实可以直接返回数据格式<userid,Aggr>
				 * 然后跟用户信息join的时候，将partAggrInffo关联上
				 */
				
				
				/*
				 * 聚合数据用什么样的格式进行拼接
				 * 
				 * 我们这里统一定义 ，使用key=value|key =value
				 */
				/*
				String partAggrInfo = Constants.FIELD_SESSION_ID+"="+sessionid+"|"
						+( StringUtils.isNotEmpty(searchKeyWords)?Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeyWords+"|":"")
						+( StringUtils.isNotEmpty(clickCategoryIds)?Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds:"");
				return new Tuple2<Long,String>(userid,partAggrInfo);*/
				
				
				
				/**
				 * TODO 为了根据实际抽取,加了一个时间start_time的field!!!并进行格式转换
				 */
				String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
						+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|"
						+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
						+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
						+ Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
						+ Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);   
				
				return new Tuple2<Long, String>(userid, partAggrInfo);
			}
			
			
		});
		
		System.out.println("userid2PartAggrInfoRDD2:"+userid2PartAggrInfoRDD.count());
		for(Tuple2<Long,String> tuple : userid2PartAggrInfoRDD.take(10)){
			System.out.println("每条数据111——"+tuple._1);
		}
		
		/**
		 * 查询所有用户数据
		 */
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		
		JavaPairRDD<Long, Row>  userid2InfoRDD  = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row row) throws Exception {
				return new Tuple2<Long ,Row>(row.getLong(0),row);
			}
			
		});
		
		System.out.println("userid2InfoRDD用户信息3:"+userid2InfoRDD.count());
		System.out.println(userid2InfoRDD);
		/*for(Tuple2<Long,Row> tuple : userid2InfoRDD.take(100)){
			System.out.println("每条数据222——"+tuple._1);
		}*/
		
		
		
		/**
		 * 【数据倾斜5】
		 * 
		 * 这里比较适合采用reduce join 抓换成map join
		 * 
		 * userid2PartAggrInfoRDD 可能数据量还是比较大的，比如1千万数据。
		 * 
		 * userid2InfoRDD 可能数据量还是比较小的，你的用户数量才10万用户
		 * 
		 * 直接调用map 和广播变量。
		 * 
		 * 根本不会shuffle ，也不会数据倾斜了！！
		 * 
		 * join中有数据倾斜第一种用这种方式！ 一个rdd比较小！！！！
		 */
		
		/*
		 * 将session粒度聚合数据，与用户信息进行join
		 */
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);
		
		System.out.println("userid2FullInfoRDD join3:"+userid2FullInfoRDD.count());
		/**
		 * 对join起来的数据进行拼接,并且返回<sessionid,fullAggrInfo>格式的数据
		 * 
		 * 
		 */
		 JavaPairRDD<String, String>  sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
			
				String partAggrInfo = tuple._2._1;
				Row userInfoRow = tuple._2._2;
				
				String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
				
				int age = userInfoRow.getInt(3);
				String professional = userInfoRow.getString(4);
				String city = userInfoRow.getString(5);
				String sex = userInfoRow.getString(6);
				
				
				String fullAggrInfo = partAggrInfo +"|"
						+Constants.FIELD_AGE+"="+age+"|"
						+Constants.FIELD_PROFESSIONAL+"="+professional+"|"
						+Constants.FIELD_CITY+"="+city+"|"
						+Constants.FIELD_SEX+"="+sex;
				
				
				return new Tuple2<String,String>(sessionid,fullAggrInfo);
			}
		});
		
		System.out.println("4:"+sessionid2FullAggrInfoRDD.count());
		
		/**
		 * 数据倾斜6
		 */
		JavaPairRDD<Long, String> sampleRDD  = userid2PartAggrInfoRDD.sample(false, 0.1,9);
		JavaPairRDD<Long, Long>  mappedSampledRDD = sampleRDD.mapToPair(new PairFunction<Tuple2<Long,String>, Long, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Long> call(Tuple2<Long, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long,Long>(tuple._1,1L);
			}
		});
		
		JavaPairRDD<Long, Long> computeSampleRDD = mappedSampledRDD.reduceByKey(new Function2<Long, Long, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		
		JavaPairRDD<Long, Long> reversedSampleRDD = computeSampleRDD.mapToPair(new PairFunction<Tuple2<Long,Long>, Long, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long,Long>(tuple._2,tuple._1);
			}
		});
		
		final Long skewedUserid = reversedSampleRDD.sortByKey(false).take(1).get(0)._2;
		
		/*
		 * 拆分得到数据倾斜的key
		 * 
		 */
		JavaPairRDD<Long, String> skewedRDD = userid2PartAggrInfoRDD.filter(new Function<Tuple2<Long,String>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<Long, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				return tuple._1.equals(skewedUserid);
			}
		});
		
		/*
		 * 拆分得到普通的key
		 */
		JavaPairRDD<Long, String> commonRDD = userid2PartAggrInfoRDD.filter(new Function<Tuple2<Long,String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<Long, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				return !tuple._1.equals(skewedUserid);
			}
		});
		
		
		/**
		 * 会发生数据倾斜的key,结合随机数打散成100份,加强版！！！！打散成100份！！
		 */
		JavaPairRDD<String, Row>  skewedUserid2infoRDD = userid2InfoRDD.filter(new Function<Tuple2<Long,Row>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<Long, Row> tuple) throws Exception {
				// TODO Auto-generated method stub
				return tuple._1.equals(skewedUserid);
			}
		}).flatMapToPair(new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple) throws Exception {
				Random random = new Random();
				List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
				for(int i=0;i<100;i++){
					int prefix = random.nextInt(100);
					list.add(new Tuple2<String, Row>(prefix + "_" + tuple._1, tuple._2));
				}
				return list;
			}
		});
		
		/*
		 * skewedRDD原来是JavaPairRDD<Long, String>  userid2PartAggrInfoRDD
		 * 
		 * skewedUserid2infoRDD 原来是userid2InfoRDD这两个要join？
		 * 
		 */
		JavaPairRDD<Long, Tuple2<String, Row>>  joinedRDD1 = skewedRDD.mapToPair(new PairFunction<Tuple2<Long,String>, String,String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
				Random random = new Random();
				int prefix = random.nextInt(100);
				return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
			}
		}).join(skewedUserid2infoRDD).mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, Long, Tuple2<String,Row>>() {

			@Override
			public Tuple2<Long, Tuple2<String, Row>> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				Long userid = Long.valueOf(tuple._1.split("_")[2]);
				return new Tuple2<Long,Tuple2<String,Row>>(userid,tuple._2);
			}
		});
		
		
		
		/*
		 * 分别join
		 */
		//JavaPairRDD<Long, Tuple2<String, Row>>  joinedRDD1 = skewedRDD.join(userid2InfoRDD);
		JavaPairRDD<Long, Tuple2<String, Row>>  joinedRDD2 = commonRDD.join(userid2InfoRDD);
		
		/*
		 * union合并
		 */
		JavaPairRDD<Long, Tuple2<String, Row>>  joinedRDD = joinedRDD1.union(joinedRDD2);
		/*
		 * 之后...map 拼接...和普通 的一样实现！
		 */
		
		
		
		
		
		/**
		 * TODO 数据倾斜5
		 * 
		 * reduce join 转化为map join
		 */
//		List<Tuple2<Long, Row>> userInfos = userid2InfoRDD.collect();
//		final Broadcast<List<Tuple2<Long, Row>>> userInfoBroadcast = sc.broadcast(userInfos);//进算子，要用final
//		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD2= userid2PartAggrInfoRDD.mapToPair(new PairFunction<Tuple2<Long,String>, String, String>() {
//
//			@Override
//			public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
//				// TODO Auto-generated method stub
//				List<Tuple2<Long, Row>> userInfos = userInfoBroadcast.value();
//				Map<Long,Row> userInfoMap = new HashMap<Long,Row>();
//				for(Tuple2<Long, Row> userInfo:userInfos){
//					userInfoMap.put(userInfo._1, userInfo._2);//(userid,userinfo)
//				}
//				
//				Long  userId= tuple._1;
//				String partAggrInfo = tuple._2;
//			/*	if(userInfoMap.containsKey(userId)){
//					
//				}*/
//				Row userInfoRow = userInfoMap.get(userId);
//				
//				
//				
//				String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//				
//				int age = userInfoRow.getInt(3);
//				String professional = userInfoRow.getString(4);
//				String city = userInfoRow.getString(5);
//				String sex = userInfoRow.getString(6);
//				String fullAggrInfo = partAggrInfo +"|"
//						+Constants.FIELD_AGE+"="+age+"|"
//						+Constants.FIELD_PROFESSIONAL+"="+professional+"|"
//						+Constants.FIELD_CITY+"="+city+"|"
//						+Constants.FIELD_SEX+"="+sex;
//				return new Tuple2<String,String>(sessionid,fullAggrInfo);
//
//			}
//		});
		
		
		
		
		
		/**
		 * 【数据倾斜7】扩容表join
		 * 
		 * 哪个要扩容?
		 * 哪个只加随机数?
		 * 
		 */
		
//		/*
//		 * 扩容
//		 */
//		JavaPairRDD<String, Row> expandedRDD = userid2InfoRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple) throws Exception {
//				List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
//				for(int i = 0; i < 10; i++) {
//					list.add(new Tuple2<String, Row>(0 + "_" + tuple._1, tuple._2));
//				}
//				return list;
//			}
//		});
//		
//		/*
//		 * 简单加随机数
//		 */
//		JavaPairRDD<String, String> mappedRDD = userid2PartAggrInfoRDD.mapToPair(new PairFunction<Tuple2<Long,String>, String, String>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
//				Random random = new Random();
//				int prefix = random.nextInt(100);
//				return new Tuple2<String,String>(prefix+"_"+tuple._1,tuple._2);
//			}
//		});
//		
//		/*
//		 * join
//		 */
//		JavaPairRDD<String, Tuple2<String, Row>> solve7joinedRDD = mappedRDD.join(expandedRDD);
//		
//		
//		/**
//		 * 解除前缀,并聚合返回最终结果
//		 */
//		JavaPairRDD<String, String> finalRDD =  solve7joinedRDD.mapToPair(
//			new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, String>() {
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public Tuple2<String, String> call(
//						Tuple2<String, Tuple2<String, Row>> tuple)
//						throws Exception {
//					String partAggrInfo = tuple._2._1;
//					Row userInfoRow = tuple._2._2;
//					
//					String sessionid = StringUtils.getFieldFromConcatString(
//							partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//					
//					int age = userInfoRow.getInt(3);
//					String professional = userInfoRow.getString(4);
//					String city = userInfoRow.getString(5);
//					String sex = userInfoRow.getString(6);
//					
//					String fullAggrInfo = partAggrInfo + "|"
//							+ Constants.FIELD_AGE + "=" + age + "|"
//							+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//							+ Constants.FIELD_CITY + "=" + city + "|"
//							+ Constants.FIELD_SEX + "=" + sex;
//					
//					return new Tuple2<String, String>(sessionid, fullAggrInfo);
//				}
//				
//			});
		
		
		return sessionid2FullAggrInfoRDD;
	}
	
	
	
	/**
	 * 过滤session数据
	 * @param sessionid2AggrInfoRDD
	 * @return
	 */
	private static JavaPairRDD<String,String> filterSessionAndAggrStat(
			JavaPairRDD<String,String> sessionid2AggrInfoRDD,
			final JSONObject taskParam,
			final Accumulator<String> sessionAggrStatAccumulator){
		
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		
		// 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
				// 此外，这里其实大家不要觉得是多此一举
				// 其实我们是给后面的性能优化埋下了一个伏笔
		String professionals =  ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		
		//String parameter = (startAge !=null ? Constants.PARAM_START_AGE+"="+startAge+"|":"");
		
		final String parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
		
		
		JavaPairRDD<String, String>  filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
			
			new Function<Tuple2<String,String>, Boolean>() {
			
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(Tuple2<String, String> tuple) throws Exception {
					/*
					 * 首先，从tuple中获取聚合数据
					 */
					String aggrInfo = tuple._2;
					
					/*
					 * 接着,依次按照筛选条件进行过滤
					 * 按照年龄范围进行过滤(startAge,endAge) 不满足 返回fasle 就把这条数据过滤掉！！
					 * 
					 */
					
					// 接着，依次按照筛选条件进行过滤
					// 按照年龄范围进行过滤（startAge、endAge）
					if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, 
							parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
						return false;
					}
					
					// 按照职业范围进行过滤（professionals）
					// 互联网,IT,软件
					// 互联网
					if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, 
							parameter, Constants.PARAM_PROFESSIONALS)) {
						return false;
					}
					
					// 按照城市范围进行过滤（cities）
					// 北京,上海,广州,深圳
					// 成都
					if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, 
							parameter, Constants.PARAM_CITIES)) {
						return false;
					}
					
					// 按照性别进行过滤
					// 男/女
					// 男，女
					if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, 
							parameter, Constants.PARAM_SEX)) {
						return false;
					}
					
					/*
					 * 按照搜索词进行过滤
					 * 我们的session可能搜索了火锅 蛋糕 烧烤
					 * 我们的筛选条件 可能是火锅 串串香，手机
					 * 那么 in这个校验方法，主要判断session搜索的词中，有任何一个与筛选条件中任何一个
					 * 搜索词相等,即通过
					 */
					if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)){
						return false;
					}
					                                                                
					//点击品类
					if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)){
						return false;
					}
					
					
					//如果经过 之前多个过滤条件之后,程序能够走到这里。
					//那么就说明,该session是通过用户指定的筛选条件的,也就是需要保留的session
					//那么就要对session的访问时长和访问步长,进行统计， 根据session对应的范围
					//进行累加
					
					sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
					//计算出session的访问时长和访问步长的范围，并进行相应的累加。
					long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
							aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
					long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
							aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
					calculateVisitLength(visitLength);
					calculateStepLength(stepLength);
					
					return true;
				}
				
				/**
				 * 计算访问时长范围
				 * @param visitLength
				 */
				private void calculateVisitLength(long visitLength) {
					if(visitLength >=1 && visitLength <= 3) {
						sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);  //找到对应的范围并对其累加1
					} else if(visitLength >=4 && visitLength <= 6) {
						sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);  
					} else if(visitLength >=7 && visitLength <= 9) {
						sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);  
					} else if(visitLength >=10 && visitLength <= 30) {
						sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);  
					} else if(visitLength > 30 && visitLength <= 60) {
						sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);  
					} else if(visitLength > 60 && visitLength <= 180) {
						sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);  
					} else if(visitLength > 180 && visitLength <= 600) {
						sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);  
					} else if(visitLength > 600 && visitLength <= 1800) {  
						sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);  
					} else if(visitLength > 1800) {
						sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);  
					} 
				}
				
				/**
				 * 计算访问步长范围
				 * @param stepLength
				 */
				private void calculateStepLength(long stepLength) {
					if(stepLength >= 1 && stepLength <= 3) {
						sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);  
					} else if(stepLength >= 4 && stepLength <= 6) {
						sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);  
					} else if(stepLength >= 7 && stepLength <= 9) {
						sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);  
					} else if(stepLength >= 10 && stepLength <= 30) {
						sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);  
					} else if(stepLength > 30 && stepLength <= 60) {
						sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);  
					} else if(stepLength > 60) {
						sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);    
					}
				}
		});
		
		/**
		 * 这里不是返回null
		 * 而是返回filteredSessionid2AggrInfoRDD过滤过的RDD
		 */
		return filteredSessionid2AggrInfoRDD;
	}
	
	
	
	
	/**
	 * 计算时长占总session比例,并进行持久化
	 * @param value
	 * @param taskid
	 */
	private static void calculateAndPersistAggrStat(String value,long taskid) {
		
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
		
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1s_3s));  
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30m));
		
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_60));
		
		// 计算各个访问时长和访问步长的范围。先转成double。
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
				(double)visit_length_1s_3s / (double)session_count, 2);  
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				(double)visit_length_4s_6s / (double)session_count, 2);  
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				(double)visit_length_7s_9s / (double)session_count, 2);  
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				(double)visit_length_10s_30s / (double)session_count, 2);  
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				(double)visit_length_30s_60s / (double)session_count, 2);  
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				(double)visit_length_1m_3m / (double)session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				(double)visit_length_3m_10m / (double)session_count, 2);  
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_10m_30m / (double)session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_30m / (double)session_count, 2);  
		
		double step_length_1_3_ratio = NumberUtils.formatDouble(
				(double)step_length_1_3 / (double)session_count, 2);  
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				(double)step_length_4_6 / (double)session_count, 2);  
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				(double)step_length_7_9 / (double)session_count, 2);  
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				(double)step_length_10_30 / (double)session_count, 2);  
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				(double)step_length_30_60 / (double)session_count, 2);  
		double step_length_60_ratio = NumberUtils.formatDouble(
				(double)step_length_60 / (double)session_count, 2);  
		
		// 将统计结果封装为Domain对象
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);  
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);  
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);  
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);  
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);  
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);  
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio); 
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);  
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio); 
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);  
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);  
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);  
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);  
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);  
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);  
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);  
		
		// 调用对应的DAO插入统计结果
		ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);  
		
	}

	
	/**
	 * 获取排名前10的热门品类
	 * @param filteredSessionid2AggrInfoRDD 过滤过的session信息
	 * @param sessionid2ActionRDD 访问明细
	 * 
	 * @param sessionid2detailRDD公共的rdd***这是被抽取出来的公共的。
	 */
	private static List<Tuple2<CategorySortKey, String>> getTop10Category(final long taskid,
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		
		/**
		 * 第一步,获取符合条件的session访问过的所有品类
		 */
		
		/*
		 * 获取符合条件的session明细信息
		 * 
		 * 抽取，独立出来！！！
		 */
/*		JavaPairRDD<String, Row>  sessionid2detailRDD  = filteredSessionid2AggrInfoRDD
			.join(sessionid2ActionRDD)
			.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {


				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String,  Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
					
					 * 放回了sessionid和action 去掉了session明细。
					 
					return new Tuple2<String,Row>(tuple._1,tuple._2._2);
				}
		});*/
		
	
		
		/*
		 * 获取session访问过的所有品类id
		 * 
		 * 访问过：指的是，点击过，下单过，支付过的品类
		 */
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
				// TODO Auto-generated method stub
				Row row = tuple._2;
				
				List<Tuple2<Long,Long>> list = new ArrayList<Tuple2<Long,Long>>();
				
				Long clickCategoryId = row.getLong(6);
				if(clickCategoryId!=null){
					list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
				}
				
				String orderCategoryIds = row.getString(8);
				if(orderCategoryIds!=null){
					String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
					for(String orderCategoryId:orderCategoryIdsSplited){
						list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId)
								,Long.valueOf(orderCategoryId)));
					}
				}
				
				String payCategoryIds = row.getString(10);
				if(payCategoryIds != null){
					String[] payCategoryIdsSplited = payCategoryIds.split(",");
					for(String payCategoryId:payCategoryIdsSplited){
						list.add(new Tuple2<Long,Long>(Long.valueOf(payCategoryId)
								,Long.valueOf(payCategoryId)));
					}
				}
				
				return null;
			}
		});
		
		/**
		 * 必须进行去重
		 * 
		 * [出现问题： 头10个都是一样的]出现重复的categoryid,排序会对重复的categoryid以及countinfo进行排序
		 * 最后可能会拿到重复的数据
		 */
		categoryidRDD = categoryidRDD.distinct();
		
		
		/**
		 * 第二步，计算各品类的点击，下单和支付的次数
		 */
		/*
		 * 访问明细中，其中三种访问行为是：点击，下单和支付
		 * 分别来计算各品类点击，下单和支付的次数，可以先对访问明细数据进行过滤。
		 * 分别过滤出点击，下单和支付行为，然后通过map，reduceByKey等算子来进行计算
		 * 
		 */
		
		//计算各个品类的点击次数
		//如果有点击,则返回，否则被过滤掉。
		/**
		 * 2.1点击
		 */
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = 
				getClickCategoryId2CountRDD(sessionid2detailRDD);
	
		//计算各个品类的点击次数
		//如果有点击,则返回，否则被过滤掉。
		/**
		 * 2.2下单
		 */
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = 
				getOrderCategoryId2CountRDD(sessionid2detailRDD);
	
			//(click,n) 计算出点击次数。
		/**
		 * 2.3支付
		 */
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = 
				getPayCategoryId2CountRDD(sessionid2detailRDD);
		
		
		/**
		 * 第三步: (leftout)join各品类与它的点击,下单,和支付次数
		 * 
		 * categoryidRDD是包含了所有符合条件的session,访问过的品类id
		 * 
		 * 上面分别计算出来的三份，各品类的点击下单和支付的次数，可能不是包含所有品类的
		 * 比如有的品类，就只是被点击过，但是没有人下单和支付
		 * 
		 * 所以这里就不能使用join操作，要使用leftJoin操作就是说，不过categoryidRDD不能join到自己的
		 * 某个数据，比如点击或者下单，那么该categoryidRDD还是要保留下来的。
		 * 
		 * 只不过没有join到的那个数据 就是0了
		 */
		JavaPairRDD<Long, String>  categoryid2countRDD = joinCategoryAndData(
				categoryidRDD,clickCategoryId2CountRDD,
				orderCategoryId2CountRDD,payCategoryId2CountRDD);
		
		
		/**
		 * 第四步: 自定义二次排序key
		 */
		
		
		/**
		 * 第五步：将数据映射成<SortKey,info>格式的RDD,然后进行二次排序
		 */
		JavaPairRDD<CategorySortKey, String>  sortKey2CountRDD  = categoryid2countRDD.mapToPair(new PairFunction<Tuple2<Long,String>, CategorySortKey, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
				String countInfo  = tuple._2;
				long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
						countInfo, "\\|", Constants.FIELD_CLICK_COUNT));  
				long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
						countInfo, "\\|", Constants.FIELD_ORDER_COUNT));  
				long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
						countInfo, "\\|", Constants.FIELD_PAY_COUNT));  
				CategorySortKey sortKey = new CategorySortKey(clickCount,orderCount,payCount);
				
				return new Tuple2<CategorySortKey,String>(sortKey,countInfo);
			}
		});
		
		JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD  = sortKey2CountRDD.sortByKey(false);
		
		/**
		 * 第六步:用take(10) 取出top10热门品类,并写入mysql
		 * TODO
		 */
		List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
	
		for(Tuple2<CategorySortKey, String> tuple:top10CategoryList){
			String countInfo = tuple._2;
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
			long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CLICK_COUNT));  
			long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_ORDER_COUNT));  
			long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_PAY_COUNT));  
			
			/*
			 * 封装一个对象
			 */
			Top10Category top10Category = 
					new Top10Category(taskid,categoryid,clickCount, orderCount,payCount);
			ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
			top10CategoryDAO.insert(top10Category);
		}
		
		return top10CategoryList;
		
	}
	
	/*
	 * 把3种获取封装成方法
	 */
	private static JavaPairRDD<Long,Long> getClickCategoryId2CountRDD(
			JavaPairRDD<String,Row> sessionid2detailRDD){
		
		
		
		/**
		 * 【算子调优2】
		 * 
		 * 这儿 是对完整的的数据进行了filter过滤。过滤出点击行为的数据。
		 * 点击行为，只占总数据的一小部分。
		 * 
		 * 而且数据量肯定变少很多。
		 * 
		 * 所以针对这种情况，还是比较合适用一下coalesce算子的。在filter过后去减少partion的数量！！
		 * 
		 * 
		 */
		JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(
				
				new Function<Tuple2<String,Row>,Boolean>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						
						/**
						 * 
						 * 出现问题,getLong(6)若为null  valueOf之后也会变成0
						 * 
						 * 改成get(6)则直接 返回null进行判断！！！
						 */
						//return Long.valueOf(row.getLong(6)) != null ? true : false;
						return row.get(6) != null ? true : false;
					}
					
						
			});
			//.coalesce(100);
			/**
			 * 说明：
			 * 
			 * 我们现在的模式是local模式，主要用来测试，所以在local模式下，不用去设置分区和并行度的数量，
			 * 
			 * local模式本身就是进程内，模拟集群执行，本身性能就很高。
			 * 
			 * 而且，对并行度，partition数量都有一定的优化。
			 * 
			 * 这里我们再自己去设置，就有点画蛇添足。
			 * 
			 * 适用于 集群！！
			 */
			
			//(clickid ,1)
			JavaPairRDD<Long, Long>  clickCategoryIdRDD  = clickActionRDD.mapToPair(
				new PairFunction<Tuple2<String,Row>,Long,Long>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) 
							throws Exception {
						long clickCategoryId = tuple._2.getLong(6);
						return new Tuple2<Long,Long>(clickCategoryId,1L);
					}
			});
			
			
			
			/**
			 * 计算各个品类的点击次数
			 * 如果某个品类点击1000万次，其他品类都是10万次，那么也会数据倾斜！！
			 */
			
			//(click,n) 计算出点击次数。
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
				new Function2<Long,Long,Long>(){
					private static final long serialVersionUID = 1L;
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
				});
			//},1000);                 //【数据倾斜3】reduce端并行度为1000
			
		
//			/**
//			 * 第一步打随机数 
//			 * 1到10以内的随机数
//			 */
//			JavaPairRDD<String, Long> mappedclickCategoryIdRDD = clickCategoryIdRDD.mapToPair(new PairFunction<Tuple2<Long,Long>, String, Long>() {
//
//				/**
//				 * 
//				 */
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public Tuple2<String, Long> call(Tuple2<Long, Long> tuple) throws Exception {
//					Random random = new Random();
//					int prefix = random.nextInt(10);
//					return new Tuple2<String,Long>(prefix+"_"+tuple._1,tuple._2);
//				}
//			});
//			/**
//			 * 执行第一轮局部聚合
//			 */
//			JavaPairRDD<String, Long>  firstAggrRDD = mappedclickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
//
//				/**
//				 * 
//				 */
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public Long call(Long v1, Long v2) throws Exception {
//					// TODO Auto-generated method stub
//					return v1+v2;
//				}
//			});
//			
//			
//			/**
//			 * 第三步去除key的前缀
//			 */
//			JavaPairRDD<Long, Long> restoreRDD = firstAggrRDD.mapToPair(new PairFunction<Tuple2<String,Long>, Long, Long>() {
//
//				@Override
//				public Tuple2<Long, Long> call(Tuple2<String, Long> tuple) throws Exception {
//					long categoryId = Long.valueOf(tuple._1.split("_")[1]);
//					return new Tuple2<Long,Long>(categoryId,tuple._2);
//				}
//			});
//			
//			/**
//			 * 做第二轮全局的聚合
//			 */
//			JavaPairRDD<Long, Long>  gloabAggrRDD = restoreRDD.reduceByKey(new Function2<Long, Long, Long>() {
//
//				/**
//				 * 
//				 */
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public Long call(Long v1, Long v2) throws Exception {
//					// TODO Auto-generated method stub
//					return v1+v2;
//				}
//			});
			
			
			
			
			return clickCategoryId2CountRDD;
		
		
		
	}
	
	
	/*
	 * 把3种获取封装成方法
	 */
	private static JavaPairRDD<Long,Long> getOrderCategoryId2CountRDD(
			JavaPairRDD<String,Row> sessionid2detailRDD){
		
		
		JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(
				
				new Function<Tuple2<String,Row>,Boolean>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return Long.valueOf(row.getString(8)) != null ? true : false;
					}
						
			});
			
			//(clickid ,1)
			
			
			//可能对多个品类下单，所以用flatmaptopair
			//flatmap作用于1的不同于map返回的是一个迭代器(1,2,3)而不是简单的(1)
			JavaPairRDD<Long, Long>  orderCategoryIdRDD  = orderActionRDD.flatMapToPair(
					new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
							Row row = tuple._2;
							String orderCategoryIds = row.getString(8);
							String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
							List<Tuple2<Long,Long>> list = new ArrayList<Tuple2<Long,Long>>();
							for(String orderCategoryId : orderCategoryIdsSplited){
								list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
							}
							return list;
						}
			});
			
			
			//(click,n) 计算出点击次数。
			JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
				new Function2<Long,Long,Long>(){

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
			});
			
			return orderCategoryId2CountRDD;
		
		
		
	}
	
	
	/*
	 * 把3种获取封装成方法
	 */
	private static JavaPairRDD<Long,Long> getPayCategoryId2CountRDD(
			JavaPairRDD<String,Row> sessionid2detailRDD){
		
		
		/*
		 * 支付次数 
		 */
				
		JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(
				
				new Function<Tuple2<String,Row>,Boolean>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return Long.valueOf(row.getString(10)) != null ? true : false;
					}
						
			});
			
			//(clickid ,1)
			
			
			//可能对多个品类下单，所以用flatmaptopair
			//flatmap作用于1的不同于map返回的是一个迭代器(1,2,3)而不是简单的(1)
			JavaPairRDD<Long, Long>  payCategoryIdRDD  = payActionRDD.flatMapToPair(
					new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
							Row row = tuple._2;
							String payCategoryIds = row.getString(10);
							String[] payCategoryIdsSplited =payCategoryIds.split(",");
							List<Tuple2<Long,Long>> list = new ArrayList<Tuple2<Long,Long>>();
							for(String payCategoryId : payCategoryIdsSplited){
								list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
							}
							return list;
						}
			});
			
			
			//(click,n) 计算出点击次数。
			JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
				new Function2<Long,Long,Long>(){

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
			});
			return payCategoryId2CountRDD;
		
		
		
	}
	
	
	
	private static JavaPairRDD<Long,String> joinCategoryAndData(
			JavaPairRDD<Long,Long> categoryidRDD,
			JavaPairRDD<Long,Long> clickCategoryId2CountRDD,
			JavaPairRDD<Long,Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long,Long> payCategoryId2CountRDD){
		
		//解释一下,如果用leftOuterJoin就可能出现右边那个RDDjoin过来时没有值
		//所以tuple中的第二个值，用optional<Long> 类型，就代表可能有值，可能没有值。
		JavaPairRDD<Long,Tuple2<Long,Optional<Long>>> tmpJoinRDD = 
				categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);
		
		JavaPairRDD<Long,String> tmpMapRDD = tmpJoinRDD.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
				long categoryid = tuple._1;
				Optional<Long>  clickCountOptional = tuple._2._2;
				long clickCount = 0L;
				if(clickCountOptional.isPresent()){
					clickCount = clickCountOptional.get();
				}
				
				String value = Constants.FIELD_CATEGORY_ID+"="+categoryid+"|"+
				Constants.FIELD_CLICK_COUNT+"=" +clickCount;
				// TODO Auto-generated method stub
				return new Tuple2<Long,String>(categoryid,value);
			}
		});
		
		/*
		tmpJoinRDD = */

		
		tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

			@Override
			public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
				long categoryid = tuple._1;
				String categoryid2click = tuple._2._1;
				Optional<Long>  orderCountOptional = tuple._2._2;
				long orderCount = 0L;
				if(orderCountOptional.isPresent()){
					orderCount = orderCountOptional.get();
				}
				
				String value = categoryid2click+"|"+
				Constants.FIELD_ORDER_COUNT+"=" +orderCount;
				return new Tuple2<Long,String>(categoryid,value);
			}
		});
		
		tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

			@Override
			public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
				long categoryid = tuple._1;
				String categoryid2click = tuple._2._1;
				Optional<Long>  payCountOptional = tuple._2._2;
				long payCount = 0L;
				if(payCountOptional.isPresent()){
					payCount = payCountOptional.get();
				}
				
				String value = categoryid2click+"|"+
				Constants.FIELD_PAY_COUNT+"=" +payCount;
				
				return new Tuple2<Long,String>(categoryid,value);
			}
		});
		
		
		/**
		 * 最终拿到品类id(id , 品类id=222|点击次数 =10|下单次数=11|支付次数=2)
		 */
		return tmpMapRDD;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	
	
}






























