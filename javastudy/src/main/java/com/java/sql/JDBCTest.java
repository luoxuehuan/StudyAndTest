package com.java.sql;

public class JDBCTest {

	public static void main(String[] args) {
		TaskDAOImpl dao = new TaskDAOImpl();
		dao.findById(1L);
	}

}
