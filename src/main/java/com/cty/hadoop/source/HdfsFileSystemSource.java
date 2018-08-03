package com.cty.hadoop.source;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.hadoop.fs.SimplerFileSystem;

import com.cty.hadoop.security.HadoopSecurityUtil;

@Configuration
public class HdfsFileSystemSource {

	private final static Logger logger = LoggerFactory.getLogger(HdfsFileSystemSource.class);
	
	private static final String NAMENODE_PRINCIPAL_KEY = "dfs.namenode.kerberos.principal";
	
	@Autowired  
	private Environment env;
	@Autowired
	private org.apache.hadoop.conf.Configuration configuration;
	
	private FileSystem fs;
	
	@Autowired
	public HdfsFileSystemSource(Environment env) {
		System.setProperty("hadoop.home.dir", env.getProperty("spring.hadoop.config.hadoop_home_dir"));
	}
	
	@Bean(name = "hdfsFS")
	@Qualifier("hdfsFS")
	public SimplerFileSystem fileSystem() throws Exception {
		
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
			
			FileSystem fs = FileSystem.get(configuration);
			return new SimplerFileSystem(fs);

		} else {
			this.fs = ((fs != null) ? fs : FileSystem.get(URI.create(fixName(configuration.get("fs.defaultFS", "hdfs://hps/"))), configuration, user));
			return new SimplerFileSystem(fs);
		}
	}
	
	
	
	private static String fixName(String name) {
		if (name.equals("local")) {
			logger.warn("\"local\" is a deprecated filesystem name. Use \"file:///\" instead.");

			name = "file:///";
		} else if (name.indexOf(47) == -1) {
			logger.warn("\"" + name + "\" is a deprecated filesystem name." + " Use \"hdfs://" + name + "/\" instead.");

			name = "hdfs://" + name;
		}
		return name;
	}
	
}
