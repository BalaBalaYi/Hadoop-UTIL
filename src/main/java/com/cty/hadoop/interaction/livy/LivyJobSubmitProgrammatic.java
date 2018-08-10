package com.cty.hadoop.interaction.livy;

import java.io.File;
import java.net.URI;
import java.util.Map;

import org.apache.livy.Job;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.cty.hadoop.job.livy.PiJob;

/**
 * 使用Livy 实现代码形式的作业提交
 * @author chentianyi
 *
 */
@Component
public class LivyJobSubmitProgrammatic {
	
	@Autowired
	private Environment env;
	
//	public Job jobWrapper() {
//		
//		Job test = new Job<T>() {
//			
//		};
//	}
	
	public boolean demo(String jarPath, Integer samples) throws Exception {
		
		String livyUrl = env.getProperty("livy.url");
		LivyClient client = new LivyClientBuilder().setURI(new URI(livyUrl)).build();

		try {
			System.err.printf("Uploading %s to the Spark context...\n", jarPath);
			client.uploadJar(new File(jarPath)).get();
			
			System.err.printf("Running PiJob with %d samples...\n", samples);
			double pi = client.submit(new PiJob(samples)).get();
			
			System.out.println("Pi is roughly: " + pi);
		} finally {
			client.stop(true);
		}
		return true;
		
	}
	
}
