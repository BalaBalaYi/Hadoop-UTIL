package com.cty.hadoop.test.hadoop;

import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.cty.hadoop.interaction.security.RangerInteraction;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RangerTests {
	
	@Autowired
	private RangerInteraction ranger;

	@Test
	public void getPolicyByServiceNameAndPolicyName() {
		Map<String, String> resultMap = ranger.getPolicyByServiceNameAndPolicyName("admin", "admin", "HPS_hadoop", "all - path");
		System.out.println("getPolicyByServiceNameAndPolicyName testing:" + resultMap.toString());
	}
}
