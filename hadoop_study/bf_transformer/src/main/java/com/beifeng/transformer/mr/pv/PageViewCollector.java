package com.beifeng.transformer.mr.pv;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.dim.base.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.mr.IOutputCollector;
import com.beifeng.transformer.service.rpc.IDimensionConverter;

/**
 * 定义pageview计算的具体输出代码
 * 
 * @author gerry
 *
 */
public class PageViewCollector implements IOutputCollector {

	
	/**
	 * @Configuration 配置文件
	 * @BaseDimension 顶级维度信息类
	 * @BaseStatsValueWritable 自定义顶级的输出value父类
	 * @PreparedStatement 数据库
	 * @IDimensionConverter 提供专门操作dimension表的接口
	 */
	@Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUser = (StatsUserDimension) key;
        int pv = ((IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1))).get();

        // 参数设置
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));//平台
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));//日期
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getBrowser()));//浏览器
        pstmt.setInt(++i, pv);// pv   1000!
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, pv);     

        // 加入batch
        pstmt.addBatch();
    }

}
