package com.chinasofti.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;


public class TestHbase {
	Configuration hconf = null;
	HBaseAdmin hba = null;
	HTable hb = null;
	HTableInterface hti = null;
	HConnection hcon = null;
	
	public TestHbase() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		hconf =  HBaseConfiguration.create();
		hcon = HConnectionManager.createConnection(hconf);
		hba = new HBaseAdmin(hcon);
	}
	
	@Test
	public void creatTable() throws IOException{
		TableName tablename = TableName.valueOf("user");
		HTableDescriptor htdc = new HTableDescriptor(tablename);
		
		HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
		HColumnDescriptor hcdData = new HColumnDescriptor("date");
		
		htdc.addFamily(hcdInfo);
		htdc.addFamily(hcdData);
		hba.createTable(htdc);
	}
	
	@Test
	public void put() throws IOException{
		hti = hcon.getTable("user");
		Put put = new Put(Bytes.toBytes("rk001"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("xiaoming"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("22"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
		put.add(Bytes.toBytes("date"), Bytes.toBytes("pic"), Bytes.toBytes("/4241243.png"));
		hti.put(put);
	}
	
	@Test
	public void get() throws IOException {
		hti = hcon.getTable("user");
		Get get = new Get(Bytes.toBytes("rk001"));
		Result rs = hti.get(get);
		Cell[] cells = rs.rawCells();
		for(Cell cell : cells){
			System.out.println(
			Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
			Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
			Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
			Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}
	
	@Test
	public void getByFamily() throws IOException {
		hti = hcon.getTable("user");
		Get get = new Get(Bytes.toBytes("rk001"));
		get.addFamily(Bytes.toBytes("info"));
		Result rs = hti.get(get);
		Cell[] cells = rs.rawCells();
		for(Cell cell : cells){
			System.out.println(
			Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
			Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
			Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
			Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}
	
	@Test
	public void getByFamilyAndCloum() throws IOException {
		hti = hcon.getTable("user");
		Get get = new Get(Bytes.toBytes("rk001"));
		get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
		Result rs = hti.get(get);
		Cell[] cells = rs.rawCells();
		for(Cell cell : cells){
			System.out.println(
			Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
			Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
			Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
			Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}
}
