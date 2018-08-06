package com.cty.hadoop.test.hadoop;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.hdfs.HdfsCommonUtil;

/**
 * Common FS 测试类
 * @author chentianyi
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class CommonFSTests {
	
	@Autowired
	private HdfsCommonUtil hdfsCom;
		
	@Test
	@Ignore
	public void hdfs() {
		String content = hdfsCom.readFileWithString("/tmp/test");
		System.out.println("testing:" + content);
	}
	

}
