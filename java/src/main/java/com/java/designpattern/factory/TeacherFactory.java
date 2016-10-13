package com.java.designpattern.factory;

public class TeacherFactory implements IDoWorkFactory{

	@Override
	public IDoWork createDoWork() {
		// TODO Auto-generated method stub
		return new TeacherDoWork();
	}

}
