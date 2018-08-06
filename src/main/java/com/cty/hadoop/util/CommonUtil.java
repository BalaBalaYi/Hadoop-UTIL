package com.cty.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class CommonUtil {
	
	private final static Logger logger = LoggerFactory.getLogger(CommonUtil.class);
	
	public String fixName(String name) {
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
