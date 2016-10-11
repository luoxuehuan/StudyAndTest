package com.ibeifeng.sparkproject.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.StringUtils;

/**
 * 模拟数据程序
 * @author Administrator
 *
 */
public class MockData {

	/**
	 * 模拟数据
	 * @param sc
	 * @param sqlContext
	 */
	public static void mock(JavaSparkContext sc,
			SQLContext sqlContext) {
		List<Row> rows = new ArrayList<Row>();
		
		/**
		 * 搜索词
		 */
		String[] searchKeywords = new String[] {"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
				"呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
		
		/**
		 * 日期
		 */
		String date = DateUtils.getTodayDate();
		
		/**
		 * 动作类型
		 */
		String[] actions = new String[]{"search", "click", "order", "pay"};
		Random random = new Random();
		
		for(int i = 0; i < 100; i++) {
			
			/**
			 * 用户id
			 */
			long userid = random.nextInt(100);    
			
			for(int j = 0; j < 10; j++) {
				
				
				/**
				 * sessionid!
				 */
				String sessionid = UUID.randomUUID().toString().replace("-", "");  
				String baseActionTime = date + " " + random.nextInt(23);
				
				
				/**
				 * 品类id
				 */
				Long clickCategoryId = null;
				  
				for(int k = 0; k < random.nextInt(100); k++) {
					
					/**
					 * 页面id
					 */
					long pageid = random.nextInt(10);    
					
					/**
					 * 动作时间
					 */
					String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
					String searchKeyword = null;
					Long clickProductId = null;
					String orderCategoryIds = null;
					String orderProductIds = null;
					String payCategoryIds = null;
					String payProductIds = null;
					
					
					/**
					 * 生成动作类型
					 */
					String action = actions[random.nextInt(4)];
					
					
					/**
					 * 如果是某一种动作类型，那就生成响应的 关键词。
					 */
					if("search".equals(action)) {
						searchKeyword = searchKeywords[random.nextInt(10)];   
					} else if("click".equals(action)) {
						if(clickCategoryId == null) {
							clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));    
						}
						clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));  
					} else if("order".equals(action)) {
						orderCategoryIds = String.valueOf(random.nextInt(100));  
						orderProductIds = String.valueOf(random.nextInt(100));
					} else if("pay".equals(action)) {
						payCategoryIds = String.valueOf(random.nextInt(100));  
						payProductIds = String.valueOf(random.nextInt(100));
					}
					
					
					/**
					 * 生成row!
					 */
					Row row = RowFactory.create(date, userid, sessionid, 
							pageid, actionTime, searchKeyword,
							clickCategoryId, clickProductId,
							orderCategoryIds, orderProductIds,
							payCategoryIds, payProductIds, 
							Long.valueOf(String.valueOf(random.nextInt(10)))); 
					
					
					/**
					 * 将生成的row加入到row list里面
					 */
					rows.add(row);
				}
			}
		}
		
		/**
		 * 把row list 转化成rdd
		 * 
		 */
		JavaRDD<Row> rowsRDD = sc.parallelize(rows);
		
		/**
		 * 准备schema
		 */
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("session_id", DataTypes.StringType, true),
				DataTypes.createStructField("page_id", DataTypes.LongType, true),
				DataTypes.createStructField("action_time", DataTypes.StringType, true),
				DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
				DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
				DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
				DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("city_id", DataTypes.LongType, true)));
		
		
		/**
		 * 把rdd注册成dataframe！
		 */
		DataFrame df = sqlContext.createDataFrame(rowsRDD, schema);
		
		df.registerTempTable("user_visit_action");  
		
		
		
		for(Row _row : df.take(1)) {
			System.out.println(_row);  
		}
		
		/**
		 * ==================================================================
		 */
		
		rows.clear();
		String[] sexes = new String[]{"male", "female"};
		for(int i = 0; i < 100; i ++) {
			long userid = i;
			String username = "user" + i;
			String name = "name" + i;
			int age = random.nextInt(60);
			String professional = "professional" + random.nextInt(100);
			String city = "city" + random.nextInt(100);
			String sex = sexes[random.nextInt(2)];
			
			Row row = RowFactory.create(userid, username, name, age, 
					professional, city, sex);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema2 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("username", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("professional", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true)));
		
		DataFrame df2 = sqlContext.createDataFrame(rowsRDD, schema2);
		for(Row _row : df2.take(1)) {
			System.out.println(_row);  
		}
		
		df2.registerTempTable("user_info");  
		
		/**
		 * ==================================================================
		 */
		rows.clear();
		
		int[] productStatus = new int[]{0, 1};
		
		for(int i = 0; i < 100; i ++) {
			long productId = i;
			String productName = "product" + i;
			String extendInfo = "{\"product_status\": " + productStatus[random.nextInt(2)] + "}";    
			
			Row row = RowFactory.create(productId, productName, extendInfo);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema3 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("product_id", DataTypes.LongType, true),
				DataTypes.createStructField("product_name", DataTypes.StringType, true),
				DataTypes.createStructField("extend_info", DataTypes.StringType, true)));
		
		DataFrame df3 = sqlContext.createDataFrame(rowsRDD, schema3);
		for(Row _row : df3.take(1)) {
			System.out.println(_row);  
		}
		
		df3.registerTempTable("product_info"); 
		 
		
		/**
		 * 测试代码
		 */
		List<Row> rowlist = new ArrayList<Row>();
		for(int i = 0 ; i< 10;i++){
			String userid = "userid";
			String keyword = "keyword";
			/**
			 * 用rowfactory来创建row
			 */
			Row row = RowFactory.create(userid,keyword);
			rowlist.add(row);
		}
		/**
		 * 把row变成RDD
		 */
		JavaRDD<Row> RowRDD = sc.parallelize(rowlist);
		
		/**
		 * 把RDD变成DataFrame
		 * 
		 * 1.需要schema
		 * 	1.1 需要List<StructField> fields valueContainsNull
		 */
		StructType  myschema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.LongType, true)));
		
		/**
		 * 构建dataframe
		 */
		DataFrame myinfo = sqlContext.createDataFrame(RowRDD, myschema);
		
		/**
		 * 注册临时表
		 */
		myinfo.registerTempTable("my_info_table");
		
		
		
	}
	
}
