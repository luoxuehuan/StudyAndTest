package com.luoxuehuan.sparkproject.domain;

/**
 * top10活跃session
 * @author Administrator
 *
 */
public class Top10Session {

	public Top10Session(long taskid, long categoryid, String sessionid, long clickCount) {
		super();
		this.taskid = taskid;
		this.categoryid = categoryid;
		this.sessionid = sessionid;
		this.clickCount = clickCount;
	}
	
	public Top10Session() {
		super();
	}

	private long taskid;
	private long categoryid;
	private String sessionid;
	private long clickCount;
	
	public long getTaskid() {
		return taskid;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public long getCategoryid() {
		return categoryid;
	}
	public void setCategoryid(long categoryid) {
		this.categoryid = categoryid;
	}
	public String getSessionid() {
		return sessionid;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public long getClickCount() {
		return clickCount;
	}
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	
}
