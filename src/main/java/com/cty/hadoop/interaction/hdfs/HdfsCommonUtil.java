package com.cty.hadoop.interaction.hdfs;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.hadoop.fs.SimplerFileSystem;
import org.springframework.stereotype.Component;

/**
 * Hdfs 文件系统常用操作
 * @author chentianyi
 *
 */
@Component
public class HdfsCommonUtil {

	private static final Logger logger = LoggerFactory.getLogger(HdfsCommonUtil.class); 
	
	@Autowired
	@Qualifier("simplerFS")
	private SimplerFileSystem fs;
	
	/**
	 * 以字符串形式追加写入文件
	 * @param filePath 文件绝对路径
	 * @param content String 类型的文件内容
	 * @return
	 */
	public boolean appendFileWithString(String filePath, String content) {
		return appendFileWithByte(filePath, content.getBytes());
	}
	
	/**
	 * 以字节数组形式追加写入文件
	 * @param filePath 文件绝对路径
	 * @param content byte[] 类型的文件内容
	 * @return
	 */
	public boolean appendFileWithByte(String filePath, byte[] content) {
		
		if(checkFileExist(filePath)) {	
			FSDataOutputStream outputStream = null;
			try {
				outputStream = fs.append(filePath, 4096);
				outputStream.write(content);
			} catch (Exception e) {
				logger.error("SimplerFileSystem appendFileWithByte failed, filePath is:" + filePath + ",content is:" + content, e);
				return false;
			} finally {
				try {
					outputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return true;
		} else {
			logger.error("SimplerFileSystem appendFileWithByte failed, file does not exsist, filePath is:" + filePath);
			return false;
		}
	}
	
	/**
	 * 读取文件，以字节数组返回
	 * @param filePath 文件绝对路径
	 * @return byte[]
	 */
	public byte[] readFileWithByte(String filePath) {
		
		byte[] fileContent = null;
		if(checkFileExist(filePath)) {
			ByteArrayOutputStream outputStream = null;
			FSDataInputStream inputStream = null;
			
			try {
				inputStream = fs.open(filePath);
				outputStream = new ByteArrayOutputStream(inputStream.available());
				IOUtils.copyBytes(inputStream, outputStream, 10240);
				fileContent = outputStream.toByteArray();
				return fileContent;
			} catch (IOException e) {
				logger.error("SimplerFileSystem readFileWithByte failed, filePath is:" + filePath, e);
				return null;
			} finally {
				IOUtils.closeStream(inputStream);
				IOUtils.closeStream(outputStream);
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
	public String readFileWithString(String filePath) {
		
		if(checkFileExist(filePath)) {
			StringBuffer fileContent = new StringBuffer();;
			FSDataInputStream inputStream = null;
			BufferedReader bufferedReader = null;
			
			try {
				inputStream = fs.open(filePath);
				bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
				String line;
				line = bufferedReader.readLine();
				while (line != null) {
					fileContent.append(line);
					line = bufferedReader.readLine();
				}
				return fileContent.toString();
			} catch (IOException e) {
				logger.error("SimplerFileSystem readFileWithString failed, filePath is:" + filePath, e);
				return null;
			} finally {
				IOUtils.closeStream(inputStream);
			}
		} else {
			return null;
		}
	}
	
	/**
	 * 检查文件或者目录是否存在
	 * @param filePath
	 * @return
	 */
	public boolean checkFileExist(String filePath) {
		
		try {
			return fs.exists(filePath);
		} catch (Exception e) {
			logger.error("SimplerFileSystem checkFileExist failed, filePath is:" + filePath, e);
			return false;
		}
	}
	
	/**
	 * 创建目录
	 * @param dirName 待创建的文件名（绝对路径）或者目录
	 * @return
	 */
	public boolean mkdir(String dirName) {
		
		if (checkFileExist(dirName))
			return true;
		try {
			logger.info("SimplerFileSystem mkdir：" + dirName);
			return fs.mkdirs(dirName);
		}
		catch (Exception e) {
			logger.error("SimplerFileSystem mkdir failed, dirName is:" + dirName, e);
			return false;
		}
	}
	
	/**
	 * 删除文件或者目录
	 * @param dirName 待删除的文件名（绝对路径）或者目录
	 */
	public void delete(String dirName) {
		
		try {
			if (fs.isDirectory(dirName)) {
				fs.delete(dirName, true);
				logger.info("SimplerFileSystem delete directory successed, directory is:" + dirName);
			} else if (fs.isFile(dirName)) {
				fs.delete(dirName, false);
				logger.info("SimplerFileSystem delete file successed, file is:" + dirName);
			}
		} catch (IOException e) {
			logger.info("SimplerFileSystem delete failed, directory or file is:" + dirName, e);
		}
	}

	/**
	 * 移动文件或者目录
	 * @param src 源路径
	 * @param dst 目标路径
	 */
	public void movefile(String src, String dst) {
		
		try {
			fs.rename(src, dst);
		} catch (IOException e) {
			logger.error("SimplerFileSystem movefile failed, source is:" + src + ",destination is:" + dst, e);
		}
	}
	
	/**
	 * 以字符串内容创建文件
	 * @param filePath
	 * @param content
	 * @return
	 */
	public boolean createFileWithString(String filePath, String content) {
		return createFileWithByte(filePath, content.getBytes());
	}
	
	
	/**
	 * 以字节内容创建文件
	 * @param filePath 待创建文件的绝对路径
	 * @param content 待创建文件的字节内容
	 * @return
	 */
	public boolean createFileWithByte(String filePath, byte[] content) {
		
		if(!checkFileExist(filePath)) {	
			FSDataOutputStream outputStream = null;
		
			try {
				outputStream = fs.create(filePath, true);
				outputStream.write(content);
			} catch (Exception e) {
				logger.error("SimplerFileSystem createFileWithByte failed, filePath is:" + filePath + ",content is:" + content, e);
				return false;
			} finally {
				try {
					outputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return true;
		} else {
			logger.error("SimplerFileSystem createFileWithByte failed, file already exsist, filePath is:" + filePath);
			return false;
		}
	}
	
	/**
	 * 复制本地文件到hdfs文件系统
	 * @param delSrc  是否删除源文件
	 * @param overwrite  是否覆盖目标地址文件
	 * @param localFile  复制的文件源路径
	 * @param hdfsPath  目标路径
	 * @return
	 */
	public boolean copyLocalFileToHDFS(boolean delSrc, boolean overwrite, String localFile, String hdfsPath) {

		try {
			fs.copyFromLocalFile(delSrc, overwrite, localFile, hdfsPath);
			return true;
		} catch (IOException e) {
//			logger.error("SimplerFileSystem copyLocalFileToHDFS failed, filePath is:" + filePath, e);
			return false;
		}
	}
	
	/**
	 * 复制hdfs文件到本地文件系统
	 * @param delSrc  是否删除源文件
	 * @param hdfsFile  复制的文件源路径
	 * @param localPath  目标路径
	 * @return
	 */
	public boolean copyHDFSFileToLocal(boolean delSrc, String hdfsFile, String localPath) {
		
		try {
			fs.copyToLocalFile(delSrc, hdfsFile, localPath);
			return true;
		} catch (IOException e) {
			logger.info("复制hdfs文件到本地文件系统发生异常！", e);
			return false;
		}
	}

}
