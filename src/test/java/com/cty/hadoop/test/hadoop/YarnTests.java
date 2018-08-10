package com.cty.hadoop.test.hadoop;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.interaction.yarn.YarnUtil;

/**
 * Yarn 常用方法测试类
 * @author chentianyi
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class YarnTests {

	@Autowired
	private YarnUtil yarnUtil;
	
	@Test
//	@Ignore
	public void listApps() {
		
//		EnumSet<YarnApplicationState> applicationStates = EnumSet.allOf(YarnApplicationState.class);
		EnumSet<YarnApplicationState> applicationStates = EnumSet.of(YarnApplicationState.RUNNING);
				
		Set<String> applicationTypes = new HashSet<String>();
		applicationTypes.add("SPARK");
		
		List result1 = yarnUtil.getAllApplications();
		List result2 = yarnUtil.getApplicationsWithTypes(applicationTypes);
		List result3 = yarnUtil.getApplicationsWithStates(applicationStates);
		List result4 = yarnUtil.getApplicationsWithNameAndTypesAndStates("Daas", applicationTypes, applicationStates);
		List result5 = yarnUtil.getApplicationsWithName("Daas");
		
//		System.out.println("all:" + result1);
//		System.out.println("type:" + result2);
//		System.out.println("state:" + result3);
		System.out.println("type+state:" + result4 + ",size:" + result4.size());
		System.out.println("name:" + result5 + ",size:" + result5.size());
	}
	
	@Test
	@Ignore
	public void killApp() {
//		System.out.println(yarnUtil.killApplicationWithApplicationId("application_1533696153379_0007"));
		System.out.println(yarnUtil.killApplicationWithApplicationName("Daas"));
	}
	
	@Test
	@Ignore
	public void getClusterTimeStamp() {
		System.out.println(yarnUtil.getClusterTimestamp());
	}
	
}
