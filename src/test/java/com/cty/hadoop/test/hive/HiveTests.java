package com.cty.hadoop.test.hive;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.interaction.hive.HiveJdbcUtil;

/**
 * Hive JDBC测试类
 * @author chentianyi
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class HiveTests {

	@Autowired
	private HiveJdbcUtil hive;
	
	@Test
	@Ignore
	public void createTable() {
		String createSql = "create table IF NOT EXISTS test1 (id int, name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";
		System.out.println(hive.ddlPlusDmlSqlExcuteWithAutoCommit(createSql));
	}
	
	@Test
//	@Ignore
	public void queryTest() {
		String querySql = "select * from phoenix_table";
		System.out.println(hive.querySqlExcute(querySql));
	}
	
}
