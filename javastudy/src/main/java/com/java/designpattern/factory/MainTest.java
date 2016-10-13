package com.java.designpattern.factory;

/**
 * 工厂模式测试
 * @author lxh
 *
 */
public class MainTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		IDoWorkFactory stuwork = new StudentFactory();
		stuwork.createDoWork().doWork();
		
		IDoWorkFactory teawork = new TeacherFactory();
		teawork.createDoWork().doWork();
	}

}
