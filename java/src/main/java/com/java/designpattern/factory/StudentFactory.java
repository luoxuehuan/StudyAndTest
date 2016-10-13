package com.java.designpattern.factory;

public class StudentFactory implements IDoWorkFactory {

	@Override
	public IDoWork createDoWork() {
		// TODO Auto-generated method stub
		return new StudentDoWork();
	}

}
