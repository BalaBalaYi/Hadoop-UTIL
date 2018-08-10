package com.cty.hadoop.test.hbase;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.interaction.hbase.PhoenixJdbcUtil;

/**
 * Phoenix JDBC测试类
 * @author chentianyi
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class PhoenixTests {
	
	@Autowired
	private PhoenixJdbcUtil phoenix;
	
	@Test
	@Ignore
	public void createTable() {
		String createSql = "create table IF NOT EXISTS test1 (id INTEGER not null primary key, name varchar)";
		System.out.println(phoenix.ddlPlusDmlSqlExcuteWithAutoCommit(createSql));
	}
	
	@Test
//	@Ignore
	public void query() {
		String querySql = "select id from test7 limit 10";
		System.out.println(phoenix.querySqlExcute(querySql));
	}

}
