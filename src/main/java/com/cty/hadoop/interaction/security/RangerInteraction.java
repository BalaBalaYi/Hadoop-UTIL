package com.cty.hadoop.interaction.security;

import java.util.HashMap;
import java.util.Map;

import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.cty.hadoop.util.CommonUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

/**
 * Ranger 常用操作
 * @author chentianyi
 *
 */
@Service
public class RangerInteraction {
	
	private static final Logger logger = LoggerFactory.getLogger(RangerInteraction.class);
	
	@Autowired
	private CommonUtil commonUtil;
	
	@Value("http://${spring.hadoop.ranger-host}:${spring.hadoop.ranger-port}")
	private String rangerBaseUrl;
	
	public Client init(String username, String password) {
		Client client = Client.create();
		client.addFilter(new HTTPBasicAuthFilter(username, password));
		return client;
	}
	
	/**
	 * 
	 * @param client
	 */
	public void destroy(ClientResponse response, Client client) {
		if(null != response) {
			response.close();
		}
		if(null != client) {
			client.destroy();
		}	
	}
	
	/**
	 * 根据服务名称和策略名称查询策略
	 * @param username
	 * @param password
	 * @param serviceName
	 * @param policyName
	 * @return
	 */
	public Map<String, String> getPolicyByServiceNameAndPolicyName(String username, String password, String serviceName, String policyName) {
		
		logger.info("RangerInteraction getPolicyByServiceNameAndPolicyName, username is:" + username + ",serviceName is:" + serviceName + ",policyName is:" + policyName);
		
		String url = rangerBaseUrl + "/service/public/v2/api/service/" + serviceName + "/policy/" + policyName + "/";
		url = url.replaceAll(" ", "%20");

		return getMethod(username, password, url);
	}
	
	/**
	 * get方法实现
	 * @param username
	 * @param password
	 * @param url
	 * @return
	 */
	public Map<String, String> getMethod(String username, String password, String url) {
		
		Client client = init(username, password);
		
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);
		
		int code = response.getStatus();
		String jsonString = response.getEntity(String.class);
		destroy(response, client);
		
		Map<String, String> resultMap = new HashMap<String, String>();
		if(authUtil(jsonString)) {
			resultMap.put("code", code + "");
			resultMap.put("msg", jsonString);
		} else {
			resultMap.put("code", "401");
			resultMap.put("msg", "Authentication fail");
		}
		return resultMap;	
	}
	
	/**
	 * 手动判断是否通过用户认证
	 * @param response
	 * @return
	 */
	public boolean authUtil(String response) {
		
		String htmlFlagString = "<html class=\"no-js\">";
		
		if(commonUtil.stringContainOrNot(response, htmlFlagString)) {
			logger.info("Ranger response msg is redirecting login pages due to authentication failed. So return 401.");
			return false;
		} else {
			return true;
		}
	}
}
