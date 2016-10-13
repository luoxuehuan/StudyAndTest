package com.java.String;

public class EnumMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(EnumTest.Fri.toString());
		
		for(EnumTest enums: EnumTest.values()){
			System.out.println(enums.getValue());
		}
		
		
		System.out.println("---------------");
		
		EnumTest test = EnumTest.Fri;
		switch(test){
			
			case Fri:
				System.out.println("friday");
				break;
			case Sun:
				System.out.println("sunday");
				break;
			default:
				System.out.println("no no no");
				break;
		}
		
	}

}
