package com.chinasofti.test;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.junit.Test;

public class TestMapReduce {
	private  final String rootDir = "hdfs://ns1/";
	private  URI rootUri = null;
	private  Configuration conf = null;
	private  FileSystem fs = null;
	private  Path outPath = null;
	private  Path inPath = null;
	
	public TestMapReduce(){
		rootUri = URI.create(rootDir);
		conf = new Configuration();
		outPath = new Path("/out");
		inPath = new Path("/hello");
		try {
			fs = FileSystem.get(rootUri,conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void jobDriver() {
		FileSystem fs;
		try {
			fs = FileSystem.get(rootUri,conf);
			if(fs.exists(outPath)){
				fs.delete(outPath,true);
			}
			
			conf.set("xmlinput.start", "<WirelessClientLocation");
			conf.set("xmlinput.end", "</WirelessClientLocation>");
			
			Job job = new Job(conf, "wordCount");
			FileInputFormat.setInputPaths(job,inPath);
			job.setInputFormatClass(XmlInputFormat.class);
			
			job.setMapperClass(MyMapper.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setPartitionerClass(HashPartitioner.class);
			
			job.setNumReduceTasks(1);
			
			job.setReducerClass(MyReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			FileOutputFormat.setOutputPath(job, outPath);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		Text text = new Text();
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String xml = value.toString();

			Document doc;
			try {
				doc = DocumentHelper.parseText(xml);

				Element rootElt = doc.getRootElement();
				String band = rootElt.attributeValue("band");
				String isGuestUser = rootElt.attributeValue("isGuestUser");
				String dot11Status = rootElt.attributeValue("dot11Status");
				String mapHierarchyString = "";
				String floorRefId = "";
				String length = "";
				String width = "";
				String imageName = "";
				String x = "";
				String y = "";
				String currentServerTime = "";
				String firstLocatedTime = "";
				Iterator iterator = rootElt.elementIterator();
				while(iterator.hasNext()){
					Element ele = (Element) iterator.next();
					String tagname = ele.getName();
					if(tagname.equals("MapInfo")){
						
						mapHierarchyString = ele.attributeValue("mapHierarchyString");
						floorRefId = ele.attributeValue("floorRefId");
						
						Iterator iterator2 = ele.elementIterator();
						while(iterator2.hasNext()){
							Element ele2 = (Element) iterator2.next();
							String tagName11 = ele2.getName();
							if(tagName11.equals("Dimension")){
								length = ele2.attributeValue("length");
								width = ele2.attributeValue("width");
								
							}else if(tagName11.endsWith("Image")){
								 imageName = ele2.attributeValue("imageName");
							}
						}
					}else if(tagname.equals("MapCoordinate")){
						 x = ele.attributeValue("x");
						 y = ele.attributeValue("y");
					}else if(tagname.equals("Statistics")){
						 currentServerTime = ele.attributeValue("currentServerTime");
						 firstLocatedTime = ele.attributeValue("firstLocatedTime");				
					}
					
				}
				String str = band+"\t"+isGuestUser+"\t"+dot11Status+"\t"+mapHierarchyString+"\t"+floorRefId+"\t"+length+"\t"
						+width+"\t"+imageName+"\t"+x+"\t"+y+"\t"+currentServerTime+"\t"+firstLocatedTime;
				System.out.println(str);
				text.set(str);
				context.write(text, NullWritable.get());
			} catch (DocumentException e) {

				e.printStackTrace();
			}
			
		};
	}
	
	static class MyReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		protected void reduce(Text key, java.lang.Iterable<LongWritable> values, Context context) throws java.io.IOException ,InterruptedException {
			context.write(key, NullWritable.get());
		};
	}
}
