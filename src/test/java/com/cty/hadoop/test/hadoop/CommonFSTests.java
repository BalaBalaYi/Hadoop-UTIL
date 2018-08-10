package com.cty.hadoop.test.hadoop;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.interaction.hdfs.HdfsCommonUtil;

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
//	@Ignore
	public void readFileWithString() {
		
		hdfsCom.createFileWithString("/tmp/seq-test2/10000-100k/testfile-0", "dingshoubinghaha");
		
		String content = hdfsCom.readFileWithString("/tmp/seq-test2/10000-100k/testfile-0");
		System.out.println("testing:" + content);
	}
	
	
	@Test
	@Ignore
	public void writeTest1() {
		// 写入 10000个 100k文件
//		for (int i = 0; i < 10000; i++) {
//			byte[] buffer = new byte[100 * 1024]; 
//			if(i == 4999){
//				byte[] buffer2 = new byte[20000 * 1024];
//				hdfsCom.createFileWithByte("/tmp/seq-test/10000-100k/testfile-" + i, buffer2);
//				continue;
//			}
//			hdfsCom.createFileWithByte("/tmp/seq-test/10000-100k/testfile-" + i, buffer);
//		}	
		
		// 写入 100个 10000k文件
		for (int i = 0; i < 100; i++) {
			byte[] buffer = new byte[10000 * 1024]; 
			hdfsCom.createFileWithByte("/tmp/seq-test/100-10000k/testfile-" + i, buffer);
		}
		
	}
	
}
