package com.chinasofti.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedAction;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.eclipse.jetty.security.jaspi.modules.UserInfo;
import org.junit.Test;



public class TestHdfs {
	private final String rootDir = "hdfs://ns1/";
	private URI rootUri = null;
	private Configuration conf = null;
	private FileSystem fs = null;
	public TestHdfs() throws InterruptedException{
		System.setProperty("user.name", "greatmap");
		rootUri = URI.create(rootDir);
		conf = new Configuration();
		try {
			fs = FileSystem.get(rootUri,conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void mikDir() throws IOException{
		
/*		UserGroupInformation ugi = UserGroupInformation.createRemoteUser("greatmap");
		ugi.createProxyUser("Administrator", UserGroupInformation.getLoginUser());*/
		UserGroupInformation ugi = UserGroupInformation.createUserForTesting("root", new String[]{"greatmap"});
	//	ugi.createProxyUser("admin", ugi);
		ugi.doAs(new PrivilegedAction<String>() {
			
		@Override
		public String run() {
			try {
				fs = FileSystem.get(rootUri,conf);
				Path path = new Path("/test201407191902");
				try {
					fs.mkdirs(path);
				} catch (IOException e) {
					e.printStackTrace();
				}finally{
					try {
						fs.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return "";
		}
		});
	}
	@Test
	public void put() throws IOException{
		Path path = new Path("/test0000/log");
		OutputStream os = fs.create(path,new Progressable() {
			
			@Override
			public void progress() {
				System.out.print(".");
				
			}
		});
		
		InputStream is = new FileInputStream("/root/log");
		IOUtils.copy(is, os);
	
	}
	@Test
	public void get() throws IOException{
		Path path = new Path("/test0000/log");
		InputStream is = fs.open(path);
		OutputStream os = new FileOutputStream("/root/log.get");
		IOUtils.copy(is, os);
	}
	
	@Test
	public void rm() throws IOException{
		Path path = new Path("/test0000/log");
		fs.deleteOnExit(path);
	}
	
	@Test
	public void list() throws FileNotFoundException, IOException{
		Path path = new Path("/");
		FileStatus[] fileStatus = fs.listStatus(path);
		for(FileStatus fst : fileStatus){
			System.out.println(fst.getPath().toString());
		}
	}
	@Test
	public void cpFromLoal() throws IOException{
		Path src = new Path("/usr/local/jdk-7u45-linux-x64.tar.gz");
		Path dst = new Path("/test0000/jdk-7u45-linux-x64.tar.gz");
		fs.copyFromLocalFile(src, dst);
	}
	
}
