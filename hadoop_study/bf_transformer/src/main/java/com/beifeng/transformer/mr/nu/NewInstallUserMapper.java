package com.beifeng.transformer.mr.nu;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.log4j.Logger;

import com.beifeng.common.DateEnum;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.dim.base.BrowserDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.mr.TransformerBaseMapper;

/**
 * 自定义的计算新用户的mapper类
 * 
 * @author gerry
 *
 */
public class NewInstallUserMapper extends TransformerBaseMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);
    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private TimeOutputValue timeOutputValue = new TimeOutputValue();
    private KpiDimension newInstallUserKpi = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
    private KpiDimension newInstallUserOfBrowserKpi = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);

    
    
    /**
     *  key代表：
     *  value代表：
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        /**
         * 父类的inputRecord 代表  输入记录数
         */
    	this.inputRecords++;
    	
    	
    	/**
    	 * 统计规则 就是统计UUID的次数
    	 * 
    	 * 根据输入的value从hbase中获取uuid (此处涉及到hbase操作,get方法)
    	 */
        String uuid = super.getUuid(value);
        String serverTime = super.getServerTime(value);//服务器时间戳
        String platform = super.getPlatform(value);  //平台名称
        if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            this.filterRecords++;
            logger.warn("uuid&servertime&platform不能为空");
            return;
        }
        long longOfTime = Long.valueOf(serverTime.trim());
        timeOutputValue.setId(uuid); // 设置id为uuid
        timeOutputValue.setTime(longOfTime); // 设置时间为服务器时间
        
        
        /**
         * 时间维度信息类
         */
        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        // 设置date维度
        StatsCommonDimension statsCommonDimension = this.statsUserDimension.getStatsCommon();
        statsCommonDimension.setDate(dateDimension);
        // 写browser相关的数据
        String browserName = super.getBrowserName(value);
        String browserVersion = super.getBrowserVersion(value);
        
        /**
         * 构建多个浏览器维度信息对象集合
         */
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);
        BrowserDimension defaultBrowser = new BrowserDimension("", "");
        for (PlatformDimension pf : platformDimensions) {
            // 1. 设置为一个默认值
            statsUserDimension.setBrowser(defaultBrowser);
            // 2. 解决有空的browser输出的bug
            // statsUserDimension.getBrowser().clean();
            statsCommonDimension.setKpi(newInstallUserKpi);
            statsCommonDimension.setPlatform(pf);
            
            /**
             * 针对不同的维度进行写出
             */
            context.write(statsUserDimension, timeOutputValue);
            this.outputRecords++;
            
            
            /**
             * platform没有变,浏览器变了。
             * 多了一个  statsUserDimension.setBrowser(br);
             * 
             */
            for (BrowserDimension br : browserDimensions) {
                statsCommonDimension.setKpi(newInstallUserOfBrowserKpi);
                // 1.
                statsUserDimension.setBrowser(br);
                // 2. 由于上面需要进行clean操作，故将该值进行clone后填充
                // statsUserDimension.setBrowser(WritableUtils.clone(br,
                // context.getConfiguration()));
                context.write(statsUserDimension, timeOutputValue);
                this.outputRecords++;
            }
        }
    }
}
