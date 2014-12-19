package com.chinasofti.test;

import com.cloudera.sqoop.Sqoop;

public class TestSqoop {
	public static void main(String[] args){
		args = new String[11];
		args[0] = "import";
		args[1] = "--connect";
		args[2] = "jdbc:mysql://hadoop03:3306/test";
		args[3] = "--username";
		args[4] = "root";
		args[5] = "--password";
		args[6] = "111111";
		args[7] = "--table";
		args[8] = "stu";
		args[9] = "-m";
		args[10] = "1";
		Sqoop.main(args);
	}
}
