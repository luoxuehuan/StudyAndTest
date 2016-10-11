package com.luoxuehuan.sparkproject.dao.factory;


import com.luoxuehuan.sparkproject.dao.IAdBlacklistDAO;
import com.luoxuehuan.sparkproject.dao.IAdUserClickCountDAO;
import com.luoxuehuan.sparkproject.dao.IAreaTop3ProductDAO;
import com.luoxuehuan.sparkproject.dao.IPageSplitConvertRateDAO;
import com.luoxuehuan.sparkproject.dao.ISessionAggrStatDAO;
import com.luoxuehuan.sparkproject.dao.ISessionDetailDAO;
import com.luoxuehuan.sparkproject.dao.ISessionRandomExtractDAO;
import com.luoxuehuan.sparkproject.dao.ITaskDAO;
import com.luoxuehuan.sparkproject.dao.ITop10CategoryDAO;
import com.luoxuehuan.sparkproject.dao.ITop10SessionDAO;
import com.luoxuehuan.sparkproject.dao.impl.AdBlacklistDAOImpl;
import com.luoxuehuan.sparkproject.dao.impl.AdUserClickCountDAOImpl;
import com.luoxuehuan.sparkproject.dao.impl.AreaTop3ProductDAOImpl;
import com.luoxuehuan.sparkproject.dao.impl.PageSplitConvertRateDAOImpl;
import com.luoxuehuan.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.luoxuehuan.sparkproject.dao.impl.SessionDetailDAOImpl;
import com.luoxuehuan.sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import com.luoxuehuan.sparkproject.dao.impl.TaskDAOImpl;
import com.luoxuehuan.sparkproject.dao.impl.Top10CategoryDAOImpl;
import com.luoxuehuan.sparkproject.dao.impl.Top10SessionDAOImpl;

public class DAOFactory {

	/**
	 * 获取任务管理DAO
	 * @return
	 */
	public static ITaskDAO getTaskDAO(){
		return new TaskDAOImpl();
	}
	
	/**
	 * 获取session聚合统计DAO
	 * @return
	 */
	public static ISessionAggrStatDAO getSessionAggrStatDAO(){
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO(){
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO(){
		return new Top10SessionDAOImpl();
	}
	
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO(){
		return new PageSplitConvertRateDAOImpl();
	}
	
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO(){
		return new AreaTop3ProductDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO(){
		return new AdUserClickCountDAOImpl();
	}
	
	public static IAdBlacklistDAO getAdBlacklistDAO(){
		return new AdBlacklistDAOImpl();
	}
	
}
