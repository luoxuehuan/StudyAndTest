package com.beifeng.transformer.mr.pv_my;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.mr.TransformerBaseMapper;


/**
 * mapper  key和value是什么
 * 
 * 进行用户分析(用户基本分析和浏览器分析)定义的组合维度
 * 
 * @author lxh
 *
 */
public class PageViewMapper extends TransformerBaseMapper<StatsUserDimension, NullWritable>{
	private static final Logger logger  = Logger.getLogger(PageViewMapper.class);
	private StatsUserDimension statsUserDimension = new StatsUserDimension();
		
	protected void map(ImmutableBytesWritable key,Result value,Context context){
		
		//1.获取platfrom、time、url
		String platform  = this.getPlatform(value);
		String serverTime = this.getServerTime(value);
		String url = this.getCurrentUrl(value);
		
		
		/**
		 * 2.过滤数据
		 * 
		 * 
		 */
		
		if(StringUtils.isBlank(platform)||StringUtils.isBlank(url)||StringUtils.isBlank(serverTime)||!StringUtils.isNumeric(serverTime.trim())){
			logger.warn("平台&服务器时间&当前url不能为空,而且服务器时间必须为时间戳的格式的字符串");
			return ;
		}
		
		/**
		 * 3.创建platform维度信息
		 */
		List<PlatformDimension> platforms = PlatformDimension.buildList(platform);
		
		/**
		 * 4.创建browser维度信息
		 */
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	}
}
