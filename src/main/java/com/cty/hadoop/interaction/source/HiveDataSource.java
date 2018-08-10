package com.cty.hadoop.interaction.source;

import java.io.IOException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.druid.pool.DruidDataSource;

/**
 * hive 数据源配置
 * @author chentianyi
 *
 */
@Configuration
public class HiveDataSource {

	@Autowired
	private Environment env;
	
	@Bean(name = "hiveJdbcDataSource")
	@Qualifier("hiveJdbcDataSource")
	public DataSource dataSource() {
		DruidDataSource dataSource = new DruidDataSource();
		dataSource.setUrl(env.getProperty("hive.url"));
		dataSource.setDriverClassName(env.getProperty("hive.driver-class-name"));
		dataSource.setUsername("hive");
		dataSource.setPassword("");
		return dataSource;
	}
	
	@Bean(name = "hiveJdbcTemplate")
	public JdbcTemplate hiveJdbcTemplate(@Qualifier("hiveJdbcDataSource") DataSource dataSource) throws IOException {
		return new JdbcTemplate(dataSource);
	}
	
	public static void main(String[] args) {
		System.out.println(System.getenv());
	}
}
