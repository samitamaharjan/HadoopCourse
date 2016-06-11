package com.mindsmapped.may;

public class Test {
	public static void main(String[] args) {
		String str = "SA5ITA";
		boolean reject = hasNumber(str);
		System.out.println(reject);
	}
	
	public static boolean hasNumber(String str)
	{
		return str.matches("[0-9]+");
	}
}
