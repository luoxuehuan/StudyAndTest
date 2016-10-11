package com.luoxuehuan.sparkproject.spark.product;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.util.SparkUtils;
import com.luoxuehuan.sparkproject.dao.IAreaTop3ProductDAO;
import com.luoxuehuan.sparkproject.dao.ITaskDAO;
import com.luoxuehuan.sparkproject.dao.factory.DAOFactory;
import com.luoxuehuan.sparkproject.domain.AreaTop3Product;

import scala.Tuple2;

public class AreaTop3ProductSpark {

	public static void main(String[] args) {
		// 创建SparkConf
				SparkConf conf = new SparkConf()
						.setAppName("AreaTop3ProductSpark");
				SparkUtils.setMaster(conf); 
				
				// 构建Spark上下文
				JavaSparkContext sc = new JavaSparkContext(conf);
				SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
				
				//【数据倾斜3】sqlContext.setConf("spark.sql.shuffle.partitions","1000");
				
				//注册自定义函数
				sqlContext.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
				
				sqlContext.udf().register("random_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
				
				sqlContext.udf().register("get_json_object", new GetJsonObjectUDF(), DataTypes.StringType);
				//注册自定义函数
				sqlContext.udf().register("group_concat_distinct", new GroupConcatDistinctUDF());
				
				
				//准备模拟数据
				SparkUtils.mockData(sc, sqlContext);
				ITaskDAO taskDAO  = DAOFactory.getTaskDAO();
				taskDAO.findById(1L);
				
				String startDate = "";
				String endDate= "";
				
				/**
				 * 查询用户指定的日期范围内的点击用户行为数据(city_id 在哪个城市发生的点击行为)
				 * 技术点： Hive数据源的使用
				 */
				JavaPairRDD<Long,Row> cityid2clickActionRDD = getcityid2ClickActionRDDByDate(sqlContext, startDate, endDate);
				
				/**
				 * 从MYSQL中查询城市信息
				 * 技术点2，异构数据源mysql的使用
				 */
				JavaPairRDD<Long,Row> cityid2cityInfoRDD = getcityid2CityInfoRDD(sqlContext);
				
				/**
				 * 生成点击商品基础信息临时表
				 * 技术点3，将rdd注册成临时表
				 */
				generateTempClickProductBasicTable(sqlContext, cityid2clickActionRDD, cityid2cityInfoRDD);
				
				/*
				 * 生成各区域各商品点击次数的临时表
				 */
				genarateAreaProductClickCountTable(sqlContext);
				
				/*
				 * 生成包含完整商品信息的各区域商品点击次数的临时表
				 */
				generateTempAreaFullProductClickCountTable(sqlContext);
				
				/*
				 * 使用开窗函数获取各个区域内点击次数排名前3的热门商品
				 */
				JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);
				
				/*
				 * 将结果写入MySQL
				 * 跟别的有点不一样,最终数据量比较小。
				 * 
				 * 总共不到10个区域,各个区域还是top3热门商品,总共最后数据量也是几十个
				 * 所以可以直接将数据collect到本地
				 * 
				 * 用批量插入的方式,一次性插入mysql即可
				 */
				List<Row> areaTop3ProductList = areaTop3ProductRDD.collect();
				
				sc.close();
	}
	


	/**
	 * 查询指定日期范围内的点击行为数据
	 * @param sqlContext 
	 * @param startDate 起始日期
	 * @param endDate 截止日期
	 * @return 点击行为数据
	 */
	private static JavaPairRDD<Long, Row> getcityid2ClickActionRDDByDate(
			SQLContext sqlContext, String startDate, String endDate) {
		// 从user_visit_action中，查询用户访问行为数据
		// 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
		// 第二个限定：在用户指定的日期范围内的数据
		
		String sql = 
				"SELECT "
					+ "city_id,"
					+ "click_product_id product_id "
				+ "FROM user_visit_action "
				+ "WHERE click_product_id IS NOT NULL "			
				+ "AND date>='" + startDate + "' "
				+ "AND date<='" + endDate + "'";
		
		DataFrame clickActionDF = sqlContext.sql(sql);
	
		JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();
	
		JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(
				
				new PairFunction<Row, Long, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {
						Long cityid = row.getLong(0);
						return new Tuple2<Long, Row>(cityid, row);  
					}
					
				});
		
		return cityid2clickActionRDD;
	}
	
	/**
	 * 使用Spark SQL从MySQL中查询城市信息
	 * @param sqlContext SQLContext
	 * @return 
	 */
	private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
		// 构建MySQL连接配置信息（直接从配置文件中获取）
		String url = null;
		String user = null;
		String password = null;
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		
		if(local) {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
		} else {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
		}
		
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", url);
		options.put("dbtable", "city_info");  
		options.put("user", user);  
		options.put("password", password);  
		
		// 通过SQLContext去从MySQL中查询数据
		DataFrame cityInfoDF = sqlContext.read().format("jdbc")
				.options(options).load();
		
		// 返回RDD
		JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
	
		JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(
			
				new PairFunction<Row, Long, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {
						long cityid = Long.valueOf(String.valueOf(row.get(0)));  
						return new Tuple2<Long, Row>(cityid, row);
					}
					
				});
		
		return cityid2cityInfoRDD;
	}
	
	/**
	 * 生成点击商品基础信息临时表
	 * @param sqlContext
	 * @param cityid2clickActionRDD
	 * @param cityid2cityInfoRDD
	 */
	private static void generateTempClickProductBasicTable(
			SQLContext sqlContext,
			JavaPairRDD<Long, Row> cityid2clickActionRDD,
			JavaPairRDD<Long, Row> cityid2cityInfoRDD) {
		// 执行join操作，进行点击行为数据和城市数据的关联
		JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD =
				cityid2clickActionRDD.join(cityid2cityInfoRDD);
		
		// 将上面的JavaPairRDD，转换成一个JavaRDD<Row>（才能将RDD转换为DataFrame）
		JavaRDD<Row> mappedRDD = joinedRDD.map(
				
				new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple)
							throws Exception {
						long cityid = tuple._1;
						Row clickAction = tuple._2._1;
						Row cityInfo = tuple._2._2;
						
						long productid = clickAction.getLong(1);
						String cityName = cityInfo.getString(1);
						String area = cityInfo.getString(2);
						
						return RowFactory.create(cityid, cityName, area, productid);  
					}
					
				});
		
		// 基于JavaRDD<Row>的格式，就可以将其转换为DataFrame
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));  
		structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));  
		
		
		/*
		 * test
		 */
		List<StructField> structfield = new ArrayList<StructField>();
		// 1 北京
		// 2 上海
		// 1 北京
		// group by area,product_id
		// 1:北京,2:上海
		
		// 两个函数
		// UDF：concat2()，将两个字段拼接起来，用指定的分隔符
		// UDAF：group_concat_distinct()，将一个分组中的多个字段值，用逗号拼接起来，同时进行去重
		
		StructType schema = DataTypes.createStructType(structFields);
	
		DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);
		System.out.println("tmp_click_product_basic: " + df.count());  
		
		// 将DataFrame中的数据，注册成临时表（tmp_click_product_basic）
		df.registerTempTable("tmp_click_product_basic");  
	}
	
	private static void genarateAreaProductClickCountTable(SQLContext sqlContext) {
		
		//按照area和product_id 两个字段进行分组
		//计算出 各区域 各商品的点击次数
		//可以获取到每个area下的每个product_id的城市信息拼接起来的串
		String sql = 
				"SELECT "
					+ "area,"
					+ "product_id,"
					+ "count(*) click_count"
					+ "group_concat_distinct(concat_long_string(cityid,cityname,',')) city_infos "  //[注意，city_infos要把数据库里的字段也改一下！！]先把cityid和city那么根据冒号拼接,再用group_concat_distinct按逗号拼接,并进行去重
				+ "FROM tmp_clk_prod_basic "
				+ "GROUP BY area,product "; 
		
		DataFrame df = sqlContext.sql(sql);
		
		//注册
		
		//各区域各商品的点击次数,以及额外的城市列表
		df.registerTempTable("tmp_area_product_click_count");
		
	}
	
	/**
	 * 生成下一个中间表
	 * 生成 区域商品点击次数临时表(包含了商品的完整信息)
	 */
	private static void generateTempAreaFullProductClickCountTable(SQLContext sqlcontext){
		
		/*
		 * 将之前得到的各区域各商品点击次数表,product_id
		 * 去关联商品信息表,product_id,product_name,product_status
		 * product_status 要特殊处理,0,1 分别代表自营和第三方的商品,放在json串里面
		 * 
		 * get_json_object()函数可以从json串中获取指定的字段的值 
		 * 
		 * 并用内置函数if 设置 0 为 自营 
		 * 
		 * area product_id click_count city_infos product_name product_status
		 * 
		 * 为什么要计算经验类型?自营?第三方?
		 * 
		 * 你拿到了某个区域top3热门商品区分经验类型很重要,你想知道是你自己的卖的还是别人的.
		 */
		
		/**
		 * 技术点,UDF,内置if
		 */
		String sql =
				"SELECT area,"
					+ "tapcc.product_id,"
					+ "tapcc.click_count,"
					+ "tapcc.city_infos,"
					+ "pi.product_name,"
					+ "if(get_json_object(pi.extend_info,'product_status')=0,'自营','第三方') product_status " 
				+ "FROM tmp_area_product_click_count tapcc "
				+ "JOIN product_info pi "
				+ "ON tapcc.product_id=pi.product_id ";
		
		DataFrame df = sqlcontext.sql(sql);
		
		df.registerTempTable("tmp_area_fullprod_click_count");
	}
	
	/**
	 * 获取各区域热门商品
	 * @param sqlContext
	 * @return
	 */
	private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext){
		
		/*
		 * 技术点
		 * 使用开窗函数进行子查询
		 * 
		 * 按照area 进行分组,给每个分组内的数据，按照点击次数降序排序，打上一个组内的行号
		 * 
		 * 接着在外层查询 过滤出各个组内的行号排名前3的数据
		 * 
		 * 其实就是各个区下的top3热门商品
		 */
		
		/*
		 * 华北,华东,华南,华中 西北 西南 东北
		 * 
		 * A级: 华北、华东                                  北京上海
		 * B级： 华南、华中
		 * C级： 西北、西南
		 * D级：东北
		 * 
		 * case wehen then ... else...end
		 */
		
		String sql = 
				"SELECT "
						+ "area,"
						+ "CASE "
							+ "WHEN area='华北' OR area='华东' THEN 'A级' "
							+ "WHEN area='华南' OR area='华中' THEN 'B级' "
							+ "WHEN area='西北' OR area='西南' THEN 'C级' "
							+ "ELSE 'D级' "//使用case when 对area进行评级 TODO
						+"END area_level,"
						+ "product_id,"
						+ "click_count,"
						+ "city_infos,"
						+ "product_name,"
						+ "product_status "
				+"FROM ("
					+"SELECT "
						+ "area,"
						+ "product_id,"
						+ "click_count,"
						+ "city_infos,"
						+ "product_name,"
						+ "product_status,"
						+ "ROW_NUMBER() OVER(PARTITION BY area BY click_count DESC) rank "//开窗函数
					+ "FROM tmp_area_fullprod_click_count "
				+") t "
				+"WHERE rank<=3";
		
		DataFrame df = sqlContext.sql(sql);
		
		
	
		return df.javaRDD();
		
		//return null;
	}
	
	
	
	
	
	/**
	 * 将计算出来的各个区域top3热门商品写入MySQL
	 * @param rows
	 */
	private static void persistAreaTop3Product(long taskid,List<Row> rows){
		
		List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();
		
		for(Row row:rows){
			AreaTop3Product areaTop3Product = new AreaTop3Product();
			areaTop3Product.setTaskid(taskid);
			areaTop3Product.setArea(row.getString(0));
			areaTop3Product.setAreaLevel(row.getString(1));
			areaTop3Product.setProductid(row.getLong(2));
			areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));//不知道什么类型直接用get(3)    
			areaTop3Product.setCityInfos(row.getString(4));  
			areaTop3Product.setProductName(row.getString(5));  
			areaTop3Product.setProductStatus(row.getString(6));  
			areaTop3Products.add(areaTop3Product);
			
			areaTop3Products.add(areaTop3Product);
		}
		
		IAreaTop3ProductDAO dao = DAOFactory.getAreaTop3ProductDAO();
		dao.insertBatch(areaTop3Products);
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
