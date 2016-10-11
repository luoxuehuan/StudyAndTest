package com.java.String;

public class SingtonString {
	
	private static SingtonString ss = null;
	/*
	 * 首先需要一个私有的构造器
	 */
	private SingtonString(){
		
	}
	/**
	 * 对外暴露一个static的获取instance的方法
	 */
	public static SingtonString getInstance(){
		if(ss == null){
			/**
			 * 为什么在这里加锁,而不是外面。
			 */
			synchronized (SingtonString.class) {
				if(ss==null){
					ss =  new SingtonString();
				}
			}
		}
		return ss;
	}
}
