package com.java.sql;

import java.sql.*;

public class DB2Conn {
	/** 设置参数 **/
	private static Connection conn = null;
	private static Statement stmt = null;
	private static ResultSet rs = null;

	/** 构造方法，链接数据库 **/
	public DB2Conn() {
		try {
			System.out.println("正在连接数据库..........");
			Class.forName("com.ibm.db2.jcc.DB2Driver");// 加载mysql驱动程序类
			String url = "jdbc:db2://10.76.64.184:50000/SC";// url为连接字符串
			String user = "administrator";// 数据库用户名
			String pwd = "1qazXSW@";// 数据库密码
			conn = (Connection) DriverManager.getConnection(url, user, pwd);
			System.out.println("数据库连接成功！！！");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			// e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
		DB2Conn a = new DB2Conn();// 实例化对象，作用是调用构造方法
		a.getClass();// 无意义
		/** 查询语句 **/
		String sql = "select * from person";
		stmt = (Statement) conn.createStatement();
		stmt.execute(sql);// 执行select语句用executeQuery()方法，执行insert、update、delete语句用executeUpdate()方法。
		rs = (ResultSet) stmt.getResultSet();
		while (rs.next()) { // 当前记录指针移动到下一条记录上
			int i = rs.getInt(1);// 得到当前记录的第一个字段(id)的值
			String name = rs.getString(2);// 得到第二个字段(name)的值
			String psw = rs.getString("ppassword");// 得到(password)的值
			System.out.println(Integer.toString(i) + " " + name + " " + psw);
		}
		rs.close();// 后定义，先关闭
		stmt.close();
		conn.close();// 先定义，后关闭
	}
}