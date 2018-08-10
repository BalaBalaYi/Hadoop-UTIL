package com.cty.hadoop.interaction.source;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.hadoop.fs.SimplerFileSystem;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.druid.pool.DruidDataSource;
import com.cty.hadoop.interaction.security.HadoopSecurityUtil;

/**
 * phoenix 数据源配置
 * @author chentianyi
 *
 */
@Configuration
public class PhoenixDataSource {
	
	private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
	
	@Autowired
	private Environment env;
	@Autowired
	private org.apache.hadoop.conf.Configuration configuration;
	
	@Bean(name = "phoenixJdbcDataSource")
	@Qualifier("phoenixJdbcDataSource")
	public DataSource dataSource() {
		DruidDataSource dataSource = new DruidDataSource();
		dataSource.setUrl(env.getProperty("phoenix.url"));
		dataSource.setDriverClassName(env.getProperty("phoenix.driver-class-name"));
		dataSource.setDefaultAutoCommit(Boolean.valueOf(env.getProperty("phoenix.default-auto-commit")));
		dataSource.setTestWhileIdle(false);
		return dataSource;
	}
	
	@Bean(name = "phoenixJdbcTemplate")
	public JdbcTemplate phoenixJdbcTemplate(@Qualifier("phoenixJdbcDataSource") DataSource dataSource) throws IOException {
		return new JdbcTemplate(dataSource);
	}
}
