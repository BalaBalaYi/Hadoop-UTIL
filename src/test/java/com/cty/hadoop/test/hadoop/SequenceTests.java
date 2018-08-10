package com.cty.hadoop.test.hadoop;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.interaction.hdfs.HdfsCommonUtil;
import com.cty.hadoop.interaction.hdfs.HdfsSequenceUtil;

/**
 * Sequence 文件类型测试类
 * @author chentianyi
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SequenceTests {

	@Autowired
	private HdfsCommonUtil commonUtil;
	
	@Autowired
	private HdfsSequenceUtil sequenceUtil;
	
	@Autowired
	@Qualifier("originalFS")
	private FileSystem fs;
	
	@Test
	@Ignore
	public void writeSeqFileWithObjectType() {
		
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
			
		String path1 = "/tmp/seq-test2";
		String path2 = "/tmp/seq-test2";
		
		String path1Src = path1 + "/100-10000k";
		String path2Src = path2 + "/10000-100k";
		
		String path1Seq = path1 + "/seq-result/100-10000k-seq";
		String path2Seq = path2 + "/seq-result/10000-100k-seq";
		
		Map<String, String> dataMap1 = new HashMap<String, String>();
		Map<String, String> dataMap2 = new HashMap<String, String>();
		
		for (int i = 0; i < 100; i++) {
			byte[] content = commonUtil.readFileWithByte(path1Src + "/testfile-" + i);
			dataMap1.put(path1Src + "/testfile-" + i, content.toString());
		}
		sequenceUtil.writeSeqFileWithTextType(dataMap1, path1Seq);
		
//		for (int i = 0; i < 10000; i++) {
//			System.out.println("开始文件" + i + "的读取:" + df.format(new Date()));
//			String content = commonUtil.readFileWithString(path2Src + "/testfile-" + i);
//			dataMap2.put(path2Src + "/testfile-" + i, content);
//			System.out.println("完成文件" + i + "的读取" + df.format(new Date()));
//		}
//		sequenceUtil.writeSeqFileWithTextType(dataMap2, path2Seq);
		
	}
	
	@Test
	@Ignore
	public void readTest() {
		
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
		
		String path1 = "/tmp/seq-test";
		String path2 = "/tmp/seq-test";
		String path1Seq = path1 + "/seq-result/100-10000k-seq";
		String path2Seq = path2 + "/seq-result/10000-100k-seq";
		
//		Map<Object, Object> dataMap1 = new HashMap<Object, Object>();
//		Map<Object, Object> dataMap2 = new HashMap<Object, Object>();
//		
//		System.out.println("开始文件1的读取:" + df.format(new Date()));
//		dataMap1 = sequenceUtil.readSeqFile(path1Seq);
//		System.out.println("完成文件1的读取:" + df.format(new Date()));
//		System.out.println("开始文件2的读取:" + df.format(new Date()));
//		dataMap2 = sequenceUtil.readSeqFile(path2Seq);
//		System.out.println("完成文件2的读取:" + df.format(new Date()));
		
		SequenceFile.Reader reader = null;
		Reader.Option[] opts = {Reader.file(new Path(path2Seq))};
		
		try {
			reader = new SequenceFile.Reader(fs.getConf(), opts);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());

			while (reader.next(key, value)) {
				System.out.println(value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}
	
	@Test
	@Ignore
	public void readSingleTest() {
		SequenceFile.Reader reader = null;
		Reader.Option[] opts = {Reader.file(new Path("/tmp/seq-test3/10000-100k-seq"))};
		
		try {
			reader = new SequenceFile.Reader(fs.getConf(), opts);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());

			while (reader.next(key, value)) {
				System.out.println(value.toString().length());
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}
	
	@Test
//	@Ignore
	public void writeSingleTest() {
		
		System.out.println("start");
		String result = commonUtil.readFileWithString("/tmp/seq-test2/xyz.txt");
//		System.out.println(result.length());
//		System.out.println(result);
		
		Map<String, String> map = new HashMap<String, String>();
		for(int i=0;i<10000;i++) {
//			byte[] content = new byte[1024 * 10000];
			map.put(i+"", result);
		}
		 System.out.println("Map集合大小为："+map.size());
		sequenceUtil.writeSeqFileWithTextType(map, "/tmp/seq-test3/10000-100k-seq");
		System.out.println("end");
	}	
	
}
