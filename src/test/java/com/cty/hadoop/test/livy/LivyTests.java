package com.cty.hadoop.test.livy;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.interaction.livy.LivyJobSubmitProgrammatic;

/**
 * Livy 测试类
 * @author chentianyi
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class LivyTests {
	
	@Autowired
	private LivyJobSubmitProgrammatic livy;
	
	@Test
//	@Ignore
	public void localJobTest () throws Exception {
		livy.demo("E:\\open source\\hadoop\\example\\PI-test.jar", 3);
	}

}
