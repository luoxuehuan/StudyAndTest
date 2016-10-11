package com.luoxuehuan.sparkproject.domain;

public class Top10Category {
	
	private long taskid;
	private long categoryid;
	private long clickCount;
	private long orderCount;
	private long payCount;

	public Top10Category(long taskid, long categoryid, long clickCount, long orderCount, long payCount) {
		super();
		this.taskid = taskid;
		this.categoryid = categoryid;
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}
	
	public final long getTaskid() {
		return taskid;
	}
	public final void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public final long getCategoryid() {
		return categoryid;
	}
	public final void setCategoryid(long categoryid) {
		this.categoryid = categoryid;
	}
	public final long getClickCount() {
		return clickCount;
	}
	public final void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	public final long getOrderCount() {
		return orderCount;
	}
	public final void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}
	public final long getPayCount() {
		return payCount;
	}
	public final void setPayCount(long payCount) {
		this.payCount = payCount;
	}
	
}
