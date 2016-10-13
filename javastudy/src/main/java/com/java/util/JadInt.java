package com.java.util;

import java.util.HashMap;
import java.util.Map;

public class JadInt {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Integer a = 1;
		Integer b = 2;
		Integer c = 3;
		Integer d = 3;
		Integer q = 310;
		Integer w = 150;
		Integer r = 150;
		Integer y = 160;
		Long e = 3L;
		System.out.println(q == (w + y));
		System.out.println(q == (y + w));
		System.out.println(c == (a + b));
		System.out.println(c.equals(a + b));
		System.out.println(e.equals(a + b));// false。。。equeal不处理数据转型。
		System.out.println(e == (a + b));// true。。。==遇到算数运算会自动拆箱

		
		Map<String,String> map = new HashMap<String,String>();
		
		Integer integer = Integer.valueOf(1);
		Integer integer1 = Integer.valueOf(2);
		Integer integer2 = Integer.valueOf(3);
		Integer integer3 = Integer.valueOf(3);
		Long long1 = Long.valueOf(3L);
		System.out.println(integer2.intValue() == integer.intValue() + integer1.intValue());
		System.out.println(integer2.equals(Integer.valueOf(integer.intValue() + integer1.intValue())));
		System.out.println(long1.equals(Integer.valueOf(integer.intValue() + integer1.intValue())));
		System.out.println(long1.longValue() == (long) (integer.intValue() + integer1.intValue()));
	}

}
