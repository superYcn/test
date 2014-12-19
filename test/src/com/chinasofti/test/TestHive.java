package com.chinasofti.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestHive {
	public static void main(String[] args) throws ClassNotFoundException, SQLException{
        Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive://hadoop02:10000/default", "", "");
        PreparedStatement pst = conn.prepareStatement("select id,username,password,age from user");
        ResultSet rst = pst.executeQuery();
        while(rst.next()){
        	System.out.println(rst.getInt("id") + "\t" +
        	rst.getString("username") + "\t" +
        	rst.getString("password") + "\t" +
        	rst.getInt("age")
        	);
        }
	}
}
