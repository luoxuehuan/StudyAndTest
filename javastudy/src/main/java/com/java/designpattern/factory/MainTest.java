package com.java.designpattern.factory;

public class MainTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		IDoWorkFactory stuwork = new StudentFactory();
		stuwork.createDoWork().doWork();
		
		IDoWorkFactory teawork = new TeacherFactory();
		teawork.createDoWork().doWork();
	}

}
