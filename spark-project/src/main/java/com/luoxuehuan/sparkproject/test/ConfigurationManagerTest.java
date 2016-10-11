package com.luoxuehuan.sparkproject.test;

import com.luoxuehuan.sparkproject.conf.ConfigurationManager;

public class ConfigurationManagerTest {

	public static void main(String[] args) {
		String testkey1 = ConfigurationManager.getProperty("key1");
		String testkey2 = ConfigurationManager.getProperty("key2");
		
		System.out.println(testkey1+testkey2);
	}

}
