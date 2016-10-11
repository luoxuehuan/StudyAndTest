package com.luoxuehuan.sparkproject.spark.product;

import java.util.Random;

import org.apache.spark.sql.api.java.UDF2;

public class RandomPrefixUDF implements UDF2<String,Integer,String>{

	@Override
	public String call(String val, Integer num) throws Exception {
		Random random = new Random();
		int ranNum = random.nextInt(num);
		return ranNum+"_"+val;
	}

}
