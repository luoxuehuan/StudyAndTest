package com.beifeng.transformer.mr.nu;

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

public class StatsUserNewInstallUserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        IntWritable newInstallUsers = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));

        
        /**
         * 根据mapping 里面写的sql来组装pstmt
         */
        /**
         *  INSERT INTO `stats_user`(
		    `platform_dimension_id`,
		    `date_dimension_id`,
		    `new_install_users`,
		    `created`)
		  VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `new_install_users` = ?
         */
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, newInstallUsers.get());//`new_install_users`,
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));// `new_install_users`
        pstmt.setInt(++i, newInstallUsers.get());//第五个参数:new_install_users` = ?
        pstmt.addBatch();//添加到...里面去
    }

}
