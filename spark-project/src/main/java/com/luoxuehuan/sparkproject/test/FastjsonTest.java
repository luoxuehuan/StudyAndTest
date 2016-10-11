package com.luoxuehuan.sparkproject.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class FastjsonTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String json = "[{'学生':'张三'},{'学生':'李四'}]";
		JSONArray jsonArray = JSONArray.parseArray(json);
		JSONObject jsonObject = jsonArray.getJSONObject(1);
		String re = jsonObject.getString("学生");
		System.out.println(re);
		
	}

}
