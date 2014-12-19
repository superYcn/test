package com.chinasofti.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;


public class Test {
	public static void main(String[] args) throws IOException{
		URL url = Test.class.getResource("/");
		String file = url.getFile();
		File file1 = new File(file,"../conf");
		System.out.println(file1.listFiles()[0].getName());
	}
}
