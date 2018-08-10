package com.cty.hadoop.interaction.source;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.hadoop.fs.SimplerFileSystem;

import com.cty.hadoop.interaction.security.HadoopSecurityUtil;
import com.cty.hadoop.util.CommonUtil;

/**
 * 文件系统初始化基类
 * @author chentianyi
 *
 */
@Configuration
public class HdfsFileSystemSource {

	private static final Logger logger = LoggerFactory.getLogger(HdfsFileSystemSource.class);
	
	private static final String NAMENODE_PRINCIPAL_KEY = "dfs.namenode.kerberos.principal";
	
	private FileSystem fs;
	
	@Autowired
	public HdfsFileSystemSource(Environment env, org.apache.hadoop.conf.Configuration configuration, CommonUtil commonUtil) throws IOException, InterruptedException {
		
		System.setProperty("hadoop.home.dir", env.getProperty("spring.hadoop.hadoop_home_dir"));
		
		String user = env.getProperty("spring.hadoop.config.user");
		String securityTpe = env.getProperty("spring.hadoop.customer.security.authMethod");

		if (null != securityTpe && "KERBEROS".equals(securityTpe)) {
			String krb5File = env.getProperty("spring.hadoop.customer.security.krb5File");
			String keytabFile = env.getProperty("spring.hadoop.customer.security.user-hdfs-keytab");
			String userPrincipal = env.getProperty("spring.hadoop.customer.security.user-hdfs-principal");
			String nnPrincipal = env.getProperty("spring.hadoop.customer.security.nn-principal");
			
			configuration.set("hadoop.security.authentication", "kerberos");
			
			HadoopSecurityUtil.setJaasConf("Client", user, keytabFile);
			HadoopSecurityUtil.setNameNodePrincipal(NAMENODE_PRINCIPAL_KEY, nnPrincipal);
			HadoopSecurityUtil.login(userPrincipal, keytabFile, krb5File, configuration);
		
			this.fs = ((fs != null) ? fs : FileSystem.get(URI.create(commonUtil.fixName(configuration.get("fs.defaultFS", "hdfs://hps/"))), configuration, user));
		} else {
			System.setProperty("HADOOP_USER_NAME", env.getProperty("spring.hadoop.config.user"));
			this.fs = ((fs != null) ? fs : FileSystem.get(URI.create(commonUtil.fixName(configuration.get("fs.defaultFS", "hdfs://hps/"))), configuration, user));
		}
	}
	
	@Bean(name = "originalFS")
	@Qualifier("originalFS")
	public FileSystem fileSystem() {
		if (null == this.fs) {
			logger.error("Error on creating FileSystem: Hadoop fs initialize failed!");
		}
		return this.fs;
	}
	
	@Bean(name = "simplerFS")
	@Qualifier("simplerFS")
	public SimplerFileSystem simplerFileSystem() {
		if (null == this.fs) {
			logger.error("Error on creating SimplerFileSystem: Hadoop fs initialize failed!");
		}
		return new SimplerFileSystem(this.fs);
	}
	
	@Bean(name = "harFS")
	@Qualifier("harFS")
	public HarFileSystem harFileSystem() {
		if (null == this.fs) {
			logger.error("Error on creating HarFileSystem: Hadoop fs initialize failed!");
		}
		return new HarFileSystem(this.fs);
	}
	
	
}
