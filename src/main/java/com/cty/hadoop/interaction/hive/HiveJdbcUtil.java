package com.cty.hadoop.interaction.hive;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Hive JDBC 接入实现类
 * @author chentianyi
 *
 */
@Component
public class HiveJdbcUtil {
	
	private final static Logger logger = LoggerFactory.getLogger(HiveJdbcUtil.class);

	@Autowired
	@Qualifier("hiveJdbcTemplate")
	private JdbcTemplate hiveJdbcTemplate;
	
	/**
	 * DDL 和 DML 等无返回 sql 执行，仅支持AutoCommit=true
	 * @param sql
	 * @return
	 */
	public boolean ddlPlusDmlSqlExcuteWithAutoCommit(String sql) {
		try {
			hiveJdbcTemplate.execute(sql);
		} catch (DataAccessException e) {
			logger.error("HiveJdbcUtil ddlPlusDmlSqlExcuteWithAutoCommit error, sql is:" + sql, e);
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
			queryResult = hiveJdbcTemplate.queryForList(sql);
		} catch (DataAccessException e) {
			logger.error("HiveJdbcUtil querySqlExcute error, sql is:" + sql, e);
			return null;
		}
		return queryResult;
	}
}
