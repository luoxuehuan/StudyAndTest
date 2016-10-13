package com.java.String;

public enum EnumTest {
	Mon(1,"d"),Tus(1,"d"),Thu(1,"d"),The(1,"d"),Fri(1,"d"),Sat(1,"d"),Sun(1,"d");
	
	
	private int value;
	private String des;
	
	EnumTest(int i,String str){
		this.value = i;
		this.des = str;
	}
	
	public String getValue(){
		return this.des;
	}

}
