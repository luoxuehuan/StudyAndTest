package com.java.sql;

import java.sql.ResultSet;



/**
 * 任务管理DAO实现类
 * @author Administrator
 *
 */
public class TaskDAOImpl{

	/**
	 * 根据主键查询任务
	 * @param taskid 主键
	 * @return 任务
	 */
	public Object findById(long taskid) {
		
		
		String sql = "select * from biz.sq_point where task_id=?";
		Object[] params = new Object[]{taskid};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
			
			@Override
			public void process(ResultSet rs) throws Exception {
				if(rs.next()) {
					long taskid = rs.getLong(1);
					String taskName = rs.getString(2);
					String createTime = rs.getString(3);
					String startTime = rs.getString(4);
					String finishTime = rs.getString(5);
					String taskType = rs.getString(6);
					String taskStatus = rs.getString(7);
					String taskParam = rs.getString(8);
					
					
				}
			}
			
		});
		return null;
	}
	
}
