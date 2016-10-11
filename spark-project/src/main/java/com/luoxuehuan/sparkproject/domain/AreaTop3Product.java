package com.luoxuehuan.sparkproject.domain;

public class AreaTop3Product {
	
	public final long getTaskid() {
		return taskid;
	}
	public final void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public final String getArea() {
		return area;
	}
	public final void setArea(String area) {
		this.area = area;
	}
	public final String getAreaLevel() {
		return areaLevel;
	}
	public final void setAreaLevel(String areaLevel) {
		this.areaLevel = areaLevel;
	}
	public final long getProductid() {
		return productid;
	}
	public final void setProductid(long productid) {
		this.productid = productid;
	}
	public final String getCityInfos() {
		return cityInfos;
	}
	public final void setCityInfos(String cityInfos) {
		this.cityInfos = cityInfos;
	}
	public final long getClickCount() {
		return clickCount;
	}
	public final void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	public final String getProductName() {
		return productName;
	}
	public final void setProductName(String productName) {
		this.productName = productName;
	}
	public final String getProductStatus() {
		return productStatus;
	}
	public final void setProductStatus(String preductStatus) {
		this.productStatus = productStatus;
	}
	private long taskid;
	private String area;
	private String areaLevel;
	private long productid;
	private String cityInfos;
	private long clickCount;
	private String productName;
	private String productStatus;

}
