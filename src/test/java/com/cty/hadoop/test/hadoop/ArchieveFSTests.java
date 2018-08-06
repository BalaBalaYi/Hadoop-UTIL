package com.cty.hadoop.test.hadoop;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.hdfs.HdfsArchieveUtil;

/**
 * Har FS 测试类
 * @author chentianyi
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ArchieveFSTests {
	
	@Autowired
	private HdfsArchieveUtil hdfsArc;
	
	private static final String harUri = "/tmp/test.har";
	private static final String fileName = "test1";
	
	@Test
	@Ignore
	public void checkFileExist() {
		boolean result = hdfsArc.checkFileExist(harUri, fileName);
		System.out.println("/tmp/test.har/test1 exist?" + result);
	}
	
	@Test
	@Ignore
	public void readFileWithString() {
		String content = hdfsArc.readFileWithString(harUri, fileName);
		System.out.println("/tmp/test.har/test1 :" + content);
	}
	
	@Test
	@Ignore
	public void makeArchive() {
		String cmd = "sudo -u hdfs hadoop archive -archiveName test.har -p /tmp/ test1 test2 /tmp/";
		hdfsArc.makeArchive("192.168.18.214", "root", "000000", cmd);
	}
}
