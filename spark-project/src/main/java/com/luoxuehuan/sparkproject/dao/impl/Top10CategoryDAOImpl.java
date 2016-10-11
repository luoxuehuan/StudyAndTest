package com.luoxuehuan.sparkproject.dao.impl;

import com.luoxuehuan.sparkproject.dao.ITop10CategoryDAO;
import com.luoxuehuan.sparkproject.domain.Top10Category;
import com.luoxuehuan.sparkproject.jdbc.JDBCHelper;

public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

	@Override
	public void insert(Top10Category category) {
		String sql = "insert into top10_category values(?,?,?,?,?)";
		
		Object[] params = new Object[]{category.getTaskid(),
				category.getCategoryid(),
				category.getClickCount(),
				category.getOrderCount(),
				category.getPayCount()
		};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
