package com.beifeng.transformer.mr.pv_my;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;

/**
 * 统计website 的page view 的reducer类
 * @author lxh
 *
 */
public class PageViewReducer extends Reducer<StatsUserDimension,NullWritable,StatsUserDimension,MapWritableValue>{
	
	private MapWritabelValue 
	

}
