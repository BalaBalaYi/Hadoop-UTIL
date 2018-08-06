package com.cty.hadoop.test.hbase;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.hbase.PhoenixUtil;

/**
 * Phoenix 测试类
 * @author chentianyi
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class PhoenixTests {
	
	@Autowired
	private PhoenixUtil phoenix;
	
	@Test
	@Ignore
	public void createTable() {
		String createSql = "create table IF NOT EXISTS test1 (id INTEGER not null primary key, name varchar)";
		System.out.println(phoenix.querySqlExcute(createSql));
	}
	
	@Test
	@Ignore
	public void query() {
		String querySql = "select * from test1";
		System.out.println(phoenix.querySqlExcute(querySql));
	}

}
