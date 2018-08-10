package com.cty.hadoop.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.cty.hadoop.interaction.hdfs.HdfsCommonUtil;

/**
 * 
 * @author chentianyi
 *
 */
@RestController("/hdfs")
public class HdfsController {
	
	@Autowired
	private HdfsCommonUtil hdfs;
	
	@RequestMapping(value = "/file/exist/{filePath}", method = RequestMethod.GET)
	@ResponseBody
	public boolean checkFileExist(@PathVariable String filePath) {
		return true;
		// 解析字符串
//		filePath.split("-");
//		
//		return hdfs.checkFileExist(filePath);
	}
}
