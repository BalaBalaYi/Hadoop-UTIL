package com.cty.hadoop.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.hbase.PhoenixUtil;
import com.cty.hadoop.hdfs.HdfsCommonUtil;
import com.cty.hadoop.hdfs.HdfsSequenceUtil;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HadoopUtilApplicationTests {
	
//	public HadoopUtilApplicationTests() {
//		System.setProperty("hadoop.home.dir", "E:\\open source\\hadoop\\hadoop-2.6.5");
//		System.setProperty("HADOOP_USER_NAME", "hdfs");
//	}
	
	@Autowired
	private HdfsCommonUtil hdfsCom;
	@Autowired
	private HdfsSequenceUtil hdfsSeq;
	
	@Autowired
	private PhoenixUtil phoenix;
	
	@Test
	@Ignore
	public void hdfs() throws IOException {
		String content = hdfsCom.readFileWithString("/tmp/test");
		System.out.println("testing:" + content);
	}
	
	@Test
//	@Ignore
	public void phoenix() throws IOException {
//		String createSql = "create table IF NOT EXISTS test1 (id INTEGER not null primary key, name varchar)";
		String querySql = "select * from test1";
		System.out.println(phoenix.querySqlExcute(querySql));
	}

}
