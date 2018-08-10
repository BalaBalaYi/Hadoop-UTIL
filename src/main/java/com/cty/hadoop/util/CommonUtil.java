package com.cty.hadoop.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class CommonUtil {
	
	private final static Logger logger = LoggerFactory.getLogger(CommonUtil.class);
	
	public String fixName(String name) {
		if (name.equals("local")) {
			logger.warn("\"local\" is a deprecated filesystem name. Use \"file:///\" instead.");

			name = "file:///";
		} else if (name.indexOf(47) == -1) {
			logger.warn("\"" + name + "\" is a deprecated filesystem name." + " Use \"hdfs://" + name + "/\" instead.");

			name = "hdfs://" + name;
		}
		return name;
	}
	
	
	public void createRandomFile(File file, long length) {
		long start = System.currentTimeMillis();
		RandomAccessFile r = null;
		try {
			r = new RandomAccessFile(file, "rw");
			r.setLength(length);
		} catch (IOException e) {
			logger.error("CommonUtil createRandomFile error", e);
		} finally{  
			if (r != null) {
				try {
					r.close();
				} catch (IOException e) {
					logger.error("CommonUtil createRandomFile error", e);
				}
			}
		 
		long end = System.currentTimeMillis();
		System.out.println(end-start);
		} 
	}
	
	
	public static void main(String[] args) {
		CommonUtil util = new CommonUtil();
		util.createRandomFile(new File("E:\\test.txt"), 1024 * 10000);
	}
}
