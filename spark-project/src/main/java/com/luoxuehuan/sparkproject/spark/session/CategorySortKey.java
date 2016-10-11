package com.luoxuehuan.sparkproject.spark.session;

import java.io.Serializable;

import scala.math.Ordered;

/**
 * 品类二次排序key
 * 
 * 封装你要排序的那几个字段
 * 
 * 实现ordered接口要求的几个方法
 * 
 * 跟其他key相比，如何来判定大于，大于等于，小于，小于等于
 * 
 * 必须实现序列化接口(否则会报错)
 * 
 * @author lxh
 *
 */
public class CategorySortKey implements Ordered<CategorySortKey> ,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long clickCount;
	private long orderCount;
	private long payCount;
	
	
	public CategorySortKey(long clickCount, long orderCount, long payCount) {
		super();
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}


	/**
	 * 大于
	 */
	@Override
	public boolean $greater(CategorySortKey other) {
		
		if(clickCount > other.getClickCount()){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount>other.getOrderCount()){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount == other.getOrderCount() &&
				payCount > other.getPayCount()){
			return true;
		}
		return false;
	}

	
	/**
	 * 大于等于
	 */
	@Override
	public boolean $greater$eq(CategorySortKey other) {
		if($greater(other)){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount == other.getOrderCount() &&
				payCount == other.getPayCount()){
			return true;
		}
		return false;
	}

	/**
	 * 小于
	 */
	@Override
	public boolean $less(CategorySortKey other) {
		if(clickCount < other.getClickCount()){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount < other.getOrderCount()){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount == other.getOrderCount() &&
				payCount < other.getPayCount()){
			return true;
		}
		return false;
	}

	/**
	 * 小于等于
	 */
	@Override
	public boolean $less$eq(CategorySortKey other) {
		if($less(other)){
			return true;
		}else if(clickCount == other.getClickCount() &&
				orderCount == other.getOrderCount() &&
				payCount == other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public int compare(CategorySortKey other) {
		if(clickCount-other.getClickCount()!=0){
			return (int)(clickCount-other.getClickCount());
		}else if(orderCount - other.getOrderCount()!=0){
			return (int)(orderCount - other.getOrderCount());
		}else if(payCount - other.getPayCount()!=0){
			return (int)(payCount - other.getPayCount());
		}
		return 0;
	}

	@Override
	public int compareTo(CategorySortKey other) {
		if(clickCount-other.getClickCount()!=0){
			return (int)(clickCount-other.getClickCount());
		}else if(orderCount - other.getOrderCount()!=0){
			return (int)(orderCount - other.getOrderCount());
		}else if(payCount - other.getPayCount()!=0){
			return (int)(payCount - other.getPayCount());
		}
		return 0;
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
