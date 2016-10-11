package com.luoxuehuan.sparkproject.dao;

import java.util.List;

import com.luoxuehuan.sparkproject.domain.SessionDetail;



public interface ISessionDetailDAO {

	/**
	 * 插入一条session明细数据
	 * @param sessionDetail 
	 */
	void insert(SessionDetail sessionDetail);
	
	/**
	 * 批量插入session明细数据
	 * @param sessionDetails
	 */
	void insertBatch(List<SessionDetail> sessionDetails);
}
