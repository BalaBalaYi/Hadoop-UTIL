package com.cty.hadoop.interaction.livy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * 使用 Livy 实现 RESTful API 形式的作业提交
 * @author chentianyi
 *
 */
@Component
public class LivyJobSubmitRestful {

	@Autowired
	private Environment env;
	
	
}
