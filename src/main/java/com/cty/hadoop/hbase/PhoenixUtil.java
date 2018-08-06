package com.cty.hadoop.hbase;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class PhoenixUtil {

	private final static Logger logger = LoggerFactory.getLogger(PhoenixUtil.class);
	
	@Autowired
	@Qualifier("phoenixJdbcTemplate")
	private JdbcTemplate phoenixJdbcTemplate;
	
	/**
	 * DDL 和 DML 等无返回 sql 执行
	 * @param sql
	 * @return
	 */
	public boolean ddlPlusDmlSqlExcute(String sql) {
		try {
			phoenixJdbcTemplate.execute(sql);
		} catch (DataAccessException e) {
			logger.error("phoenixJdbcTemplate ddlPlusDmlSqlExcute error, sql is:" + sql, e);
			return false;
		}
		return true;
	}
	
	/**
	 * 查询 sql 执行
	 * @param sql
	 * @return
	 */
	public List<Map<String, Object>> querySqlExcute(String sql) {
		
		List<Map<String, Object>> queryResult = null;
		try {
			queryResult = phoenixJdbcTemplate.queryForList(sql);
		} catch (DataAccessException e) {
			logger.error("phoenixJdbcTemplate querySqlExcute error, sql is:" + sql, e);
			return null;
		}
		return queryResult;
	}
}
