package com.cty.hadoop.interaction.hdfs;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.springframework.stereotype.Component;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

/**
 * Hdfs 文件系统Archieve文件常用操作
 * @author chentianyi
 *
 */
@Component
public class HdfsArchieveUtil {

	private static final Logger logger = LoggerFactory.getLogger(HdfsArchieveUtil.class);
	
	@Autowired
	@Qualifier("harFS")
	private HarFileSystem harFs;
	
	@Autowired
	@Qualifier("originalFS")
	private FileSystem fs;
	
	public void init(String harUri) {
		try {
			this.harFs.initialize(new URI(harUri), fs.getConf());
		} catch (Exception e) {
			logger.error("HarFileSystem initialize failed, harUri is:" + harUri, e);
			close();
		}
	}
	
	public void close(){
		try {
			this.harFs.close();
		} catch (IOException e) {
			logger.error("HarFileSystem close failed", e);
		}
	}
	
	/**
	 * 检查压缩包下是否存在指定文件
	 * @param harUri e.g. /tmp/test.har
	 * @param fileName e.g. test1 (/tmp/test.har/test1)
	 * @return boolean
	 */
	public boolean checkFileExist(String harUri, String fileName) {
		
		init(harUri);
		
		boolean result = false;
		try {
			result = harFs.exists(new Path(harUri + "/" + fileName));
		} catch (Exception e) {
			logger.error("HarFileSystem checkFileExist failed, harUri is:" + harUri + ",fileName is:" + fileName, e);
		} finally {
			close();
		}
		return result;
	}
	

	/**
	 * 读取压缩包中的文件，以字节数组返回
	 * @param harUri 
	 * @param fileName 
	 * @return byte[]
	 */
	public byte[] readFileWithByte(String harUri, String fileName) {
		
		byte[] fileContent = null;
		if(checkFileExist(harUri, fileName)) {
			ByteArrayOutputStream outputStream = null;
			FSDataInputStream inputStream = null;
			
			try {
				inputStream = harFs.open(new Path(harUri + "/" + fileName));
				outputStream = new ByteArrayOutputStream(inputStream.available());
				IOUtils.copyBytes(inputStream, outputStream, 4096);
				fileContent = outputStream.toByteArray();
				return fileContent;
			} catch (Exception e) {
				logger.error("HarFileSystem readFileWithByte failed, harUri is:" + harUri + ",fileName is:" + fileName, e);
				return null;
			} finally {
				IOUtils.closeStream(inputStream);
				IOUtils.closeStream(outputStream);
				close();
			}
		} else {
			return null;
		}
	}
	
	/**
	 * 读取文件，以字符串返回
	 * @param filePath 文件绝对路径
	 * @return String
	 */
	public String readFileWithString(String harUri, String fileName) {
		
		if(checkFileExist(harUri, fileName)) {
			StringBuffer fileContent = new StringBuffer();;
			FSDataInputStream inputStream = null;
			
			try {
				inputStream = harFs.open(new Path(harUri + "/" + fileName));
				byte[] b = new byte[4096];
				while (inputStream.read(b) != -1) {
					fileContent.append(new String(b));
				}
				
				return fileContent.toString();
			} catch (IOException e) {
				logger.error("HarFileSystem readFileWithString failed, harUri is:" + harUri + ",fileName is:" + fileName, e);
				return null;
			} finally {
				IOUtils.closeStream(inputStream);
				close();
			}
		} else {
			return null;
		}
	}
	
	/**
	 * 通过远程执行  hadoop archive 命令创建 har 文件
	 * @param hostIPAddress
	 * @param userName
	 * @param userPassword
	 * @param cmd e.g. sudo -u hdfs hadoop archive -archiveName test.har -p /tmp/ test1 test2 /tmp/
	 * @throws Exception
	 */
	public boolean makeArchive(String hostIPAddress, String userName, String userPassword, String cmd) {
		
		Connection conn = new Connection(hostIPAddress);
		boolean authenticateVal = false;
		try {
			conn.connect();
			authenticateVal = conn.authenticateWithPassword(userName, userPassword);
		} catch (IOException e) {
			logger.error("HarFileSystem makeArchive failed, can not connect to the remote host:" + hostIPAddress, e);
			return false;
		}
		
		if(authenticateVal) {	
			Session session = null;
			try {
				session = conn.openSession();
				session.execCommand(cmd);
			} catch (IOException e) {
				logger.error("HarFileSystem makeArchive failed, cmd execute failed, cmd is:" + cmd, e);
				return false;
			}
			InputStream stdout = new StreamGobbler(session.getStdout());
			BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
			while (true) {
				String line = null;
				try {
					line = br.readLine();
					if (line == null)
						break;
					logger.debug(line);
				} catch (IOException e) {
					logger.warn("HarFileSystem makeArchive exception, stdout was going wrong");
				}	
			}
			int resultCode = session.getExitStatus();
			session.close();
			conn.close();
			
			if(0 == resultCode) {
				logger.info("HarFileSystem makeArchive successed, cmd is:" + cmd);
				return true;
			} else {
				logger.error("HarFileSystem makeArchive failed, cmd is:" + cmd + ". Please check ur cmd!");
				return false;
			}
			
		} else {
			logger.error("HarFileSystem makeArchive failed, can not connect to the remote host:" + hostIPAddress + ",auth failed with username:" + userName + " and password:" + userPassword);
			return false;
		}

	}

}
