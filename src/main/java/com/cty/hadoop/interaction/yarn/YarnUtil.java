package com.cty.hadoop.interaction.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Yarn 常用操作类
 * @author chentianyi
 *
 */
@Component
public class YarnUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(YarnUtil.class);

	@Autowired
	@Qualifier("originalFS")
	private FileSystem fs;
	
	/**
	 * 创建客户端连接
	 * @return
	 */
	public YarnClient createClient() {
		YarnClient client = YarnClient.createYarnClient();
		client.init(fs.getConf());
		client.start();
		return client;
	}
	
	/**
	 * 销毁客户端连接
	 * @param client
	 */
	public void closeClient(YarnClient client) {
		try {
			client.close();
		} catch (IOException e) {
			logger.error("YarnUtil closeClient error", e);
		}
	}
	
	/**
	 * 查询所有作业
	 * @return
	 */
	public List<ApplicationReport> getAllApplications() {
		return getApplicationsWithNameAndTypesAndStates(null, null, null);
	}
	
	/**
	 * 根据作业名称查询作业
	 * @param applicationName
	 * @return
	 */
	public List<ApplicationReport> getApplicationsWithName(String applicationName) {
		return getApplicationsWithNameAndTypesAndStates(applicationName, null, null);
	}
	
	/**
	 * 根据作业类型查询作业
	 * @param applicationTypes e.g. YARN SPARK MAPREDUCE	
	 * @return
	 */
	public List<ApplicationReport> getApplicationsWithTypes(Set<String> applicationTypes) {
		return getApplicationsWithNameAndTypesAndStates(null, applicationTypes, null);
	}
	
	/**
	 * 根据作业状态查询作业
	 * @param applicationStates: ACCEPTED FAILED FINISHED KILLED NEW NEW_SAVING RUNNING SUBMITTED
	 * @return
	 */
	public List<ApplicationReport> getApplicationsWithStates(EnumSet<YarnApplicationState> applicationStates) {
		return getApplicationsWithNameAndTypesAndStates(null, null, applicationStates);
	}
	
	/**
	 * 根据作业名称、作业类型和作业状态查询作业
	 * @param applicationName
	 * @param applicationTypes e.g. YARN SPARK MAPREDUCE	
	 * @param applicationStates: ACCEPTED FAILED FINISHED KILLED NEW NEW_SAVING RUNNING SUBMITTED
	 * @return
	 */
	public List<ApplicationReport> getApplicationsWithNameAndTypesAndStates(String applicationName, Set<String> applicationTypes, EnumSet<YarnApplicationState> applicationStates) {
		YarnClient client = createClient();
		List<ApplicationReport> applicationList;
		
		try {
			applicationList = client.getApplications(applicationTypes, applicationStates);
			
			// 名称筛选
			if(null != applicationName) {
				List<ApplicationReport> resultList = new ArrayList<ApplicationReport>();
				
				for(ApplicationReport appReport : applicationList) {
					if(applicationName.equals(appReport.getName())) {
						resultList.add(appReport);
					}
				}
				return resultList;
			} else {
				return applicationList;
			}
		} catch (YarnException | IOException e) {
			logger.error("YarnUtil getApplicationsWithNameAndTypesAndStates error, applicationTypes is:" + applicationTypes.toString() 
				+ ",applicationStates is:" + applicationStates.toString(), e);
			return null;
		} finally {
			closeClient(client);
		}
	}
	
	/**
	 * 根据字符串形式的 application id 结束作业
	 * @param applicationId e.g. application_1533696153379_0002
	 * @return
	 */
	public boolean killApplicationWithApplicationId(String applicationId) {
		
		if(null == applicationId) {
			logger.error("YarnUtil killApplicationWithApplicationId error, applicationId is null");
			return false;
		}
	
		String[] applicationIdStr = applicationId.split("_");
		long clusterTimeStamp = Long.parseLong(applicationIdStr[1]);
		int id = Integer.parseInt(applicationIdStr[2]);
		
		return killApplicationWithTimeStampAndId(clusterTimeStamp, id);
	}
	
	/**
	 * 根据字符串形式的 application name 结束作业
	 * @param applicationName
	 * @return
	 * 
	 * 备注：只能结束第一个匹配名称的作业
	 */
	@Deprecated
	public boolean killApplicationWithApplicationName(String applicationName) {
		
		EnumSet<YarnApplicationState> applicationStates = EnumSet.of(YarnApplicationState.RUNNING);
		List<ApplicationReport> runningResultList =getApplicationsWithStates(applicationStates);
		
		ApplicationId applicationId= null;
		
		for(ApplicationReport appReport : runningResultList) {
			if(applicationName.equals(appReport.getName())) {
				applicationId = appReport.getApplicationId();
				break;
			}
		}
		return killApplication(applicationId);
	}
	
	/**
	 * 根据application id 的时间戳和id结束作业
	 * @param clusterTimeStamp e.g. 1533696153379
	 * @param id e.g. 0002
	 * @return
	 */
	public boolean killApplicationWithTimeStampAndId(long clusterTimeStamp, int id) {
		return killApplication(ApplicationId.newInstance(clusterTimeStamp, id));	
	}
	
	/**
	 * 根据标准类型application id结束作业
	 * @param applicationId
	 * @return
	 */
	public boolean killApplication(ApplicationId applicationId) {
		
		YarnClient client = createClient();
		
		try {
			client.killApplication(applicationId);
		} catch (YarnException | IOException e) {
			logger.error("YarnUtil killApplication error, applicationId is:" + applicationId.toString(), e);
			return false;
		} finally {
			closeClient(client);
		}
		return true;
	}

	
	public long getClusterTimestamp() {
		YarnClient client = createClient();
		long clusterTimeStamp = 0;
		try {
			clusterTimeStamp = client.getStartTime();
		} finally {
			closeClient(client);
		}
		return clusterTimeStamp;
	}
	
}
