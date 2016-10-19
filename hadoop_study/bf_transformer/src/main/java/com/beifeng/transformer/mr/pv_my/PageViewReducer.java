package com.beifeng.transformer.mr.pv_my;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;

/**
 * 统计website 的page view 的reducer类
 * @author lxh
 *
 */
public class PageViewReducer extends Reducer<StatsUserDimension,NullWritable,StatsUserDimension,MapWritableValue>{
	
	private MapWritableValue mapWritableValue = new MapWritableValue();
	
	private MapWritable map = new MapWritable();
	
	protected void reduce (StatsUserDimension key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException{
		int pvCount = 0;
		
		/**
		 * 有一个输入统计一次输入 pvCount+1
		 */
		for(NullWritable value : values){
			  pvCount ++;      
		}
		
		/**
		 * 填充value
		 */
		this.map.put(new IntWritable(-1),new IntWritable(pvCount));
		this.mapWritableValue.setValue(this.map);
		
		/**
		 * 填充kpi
		 */
		this.mapWritableValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()     ));
		
		//输出
		
		context.write(key, this.mapWritableValue);
	}
	

}
