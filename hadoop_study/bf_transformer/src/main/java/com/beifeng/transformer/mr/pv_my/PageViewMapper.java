package com.beifeng.transformer.mr.pv_my;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.beifeng.common.DateEnum;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.dim.base.BrowserDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
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
	private KpiDimension websitePageViewDemension = new KpiDimension(KpiType.WEBSITE_PAGEVIEW.name);
	
	protected void map(ImmutableBytesWritable key,Result value,Context context) throws IOException, InterruptedException{
		
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
		String browserName = this.getBrowserName(value);
		String browserVersion = this.getBrowserVersion(value);
		
		List<BrowserDimension> browsers = BrowserDimension.buildList(browserName, browserVersion);
		
		/**
		 * 创建date维度信息。
		 */
		DateDimension dayOfDimension = DateDimension.buildDate(Long.valueOf(serverTime.trim()), DateEnum.DAY);
		
		//6. 输出的写出。
		StatsCommonDimension statCommon = this.statsUserDimension.getStatsCommon();
		
		statCommon.setDate(dayOfDimension);
		statCommon.setKpi(this.websitePageViewDemension);
		
		for(PlatformDimension pf : platforms){
			statCommon.setPlatform(pf);
			for(BrowserDimension br : browsers){
				this.statsUserDimension.setBrowser(br);
				//输出
				context.write(this.statsUserDimension, NullWritable.get());
			}
		}
		
		
		
		
		
		
	}
}
