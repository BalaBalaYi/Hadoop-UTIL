package com.cty.hadoop.hdfs;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.hadoop.fs.SimplerFileSystem;
import org.springframework.stereotype.Component;

/**
 * Hdfs 文件系统Sequence文件常用操作
 * @author chentianyi
 *
 */
@Component
public class HdfsSequenceUtil {

	private static final Logger logger = LoggerFactory.getLogger(HdfsSequenceUtil.class);
	
	@Autowired
	@Qualifier("simplerFS")
	private SimplerFileSystem fs;
	
	/**
	 * 根据序列化类型 Text，写入sequence file
	 * @param dataMap 数据内容
	 * @param filePath 写入文件目标地址
	 */
	public boolean writeSeqFileWithTextType(Map<String, String> dataMap, String filePath) {
		
		SequenceFile.Writer writer = null;
		Writer.Option[] opts = {Writer.file(new Path(filePath)), SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class)};
		
		try {
			writer = SequenceFile.createWriter(fs.getConf(), opts);
			for (String key : dataMap.keySet()) {
				writer.append(new Text(key), new Text(dataMap.get(key)));
			}
		} catch (Exception e) {
			logger.error("写入sequence file 发生异常！", e);
			return false;
		} finally {
			IOUtils.closeStream(writer);
		}
		return true;
	}
	
	/**
	 * 根据序列化类型 ObjectWritable，写入sequence file
	 * @param dataMap 数据内容
	 * @param filePath 写入文件目标地址
	 */
	public boolean writeSeqFileWithObjectType(Map<Object, Object> dataMap, String filePath) {
		
		SequenceFile.Writer writer = null;
		Writer.Option[] opts = {Writer.file(new Path(filePath)), SequenceFile.Writer.keyClass(ObjectWritable.class), SequenceFile.Writer.valueClass(ObjectWritable.class)};
		
		try {
			writer = SequenceFile.createWriter(fs.getConf(), opts);
			for (Object key : dataMap.keySet()) {
				writer.append(new ObjectWritable(key), new ObjectWritable(dataMap.get(key)));
			}
		} catch (Exception e) {
			logger.error("写入sequence file 发生异常！", e);
			return false;
		} finally {
			IOUtils.closeStream(writer);
		}
		return true;
	}
	
	/**
	 * 根据传入dataMap 中的数据类型，自动映射为对应的序列化类型 ，写入sequence file
	 * @param dataMap 数据内容
	 * @param filePath 写入文件目标地址
	 */
	public boolean writeSeqFileWithAutoSpecifiedType(Map<? extends Object, ? extends Object> dataMap, String filePath) {

		// 类型判断
		if(dataMap.isEmpty()) {
			logger.warn("写入sequence file 内容为空！");
			return false;
		}
		Class<? extends Object> keyClazz = null;
		Class<? extends Object> valClazz = null;
		for(Object key : dataMap.keySet()) {
			keyClazz = key.getClass();
			valClazz = dataMap.get(key).getClass();
			break;
		}
		
		Class<? extends Writable> keyWritableClazz = mappingWritableClazz(keyClazz);
		Class<? extends Writable> valWritableClazz = mappingWritableClazz(valClazz);
		
		keyClazz = transferToPrimitive(keyClazz);
		valClazz = transferToPrimitive(valClazz);
		
		SequenceFile.Writer writer = null;
		Writer.Option[] opts = {Writer.file(new Path(filePath)), SequenceFile.Writer.keyClass(keyWritableClazz), SequenceFile.Writer.valueClass(valWritableClazz)};
		
		try {
			writer = SequenceFile.createWriter(fs.getConf(), opts);
			for (Object key : dataMap.keySet()) {
				Constructor<? extends Writable> keyWritableClazzConstructor = keyWritableClazz.getConstructor(keyClazz);
				Constructor<? extends Writable> valWritableClazzConstructor = valWritableClazz.getConstructor(valClazz);
				writer.append(keyWritableClazzConstructor.newInstance(key), valWritableClazzConstructor.newInstance(dataMap.get(key)));
			}
		} catch (Exception e) {
			logger.error("写入sequence file 发生异常！", e);
			return false;
		} finally {
			IOUtils.closeStream(writer);
		}
		return true;
	}
	
	/**
	 * 根据自定义序列化类型，写入sequence file
	 * @param dataMap 数据内容
	 * @param filePath 写入文件目标地址 
	 * @param keyClazz Writable子类型
	 * @param valClazz Writable子类型
	 */
	@Deprecated
	public boolean writeSeqFileWithManualSpecifiedType(Map<Object, Object> dataMap, String filePath, Class<? extends Writable> keyClazz, Class<? extends Writable> valClazz) {
		
		SequenceFile.Writer writer = null;
		Writer.Option[] opts = {Writer.file(new Path(filePath)), SequenceFile.Writer.keyClass(keyClazz), SequenceFile.Writer.valueClass(valClazz)};
		
		try {
			writer = SequenceFile.createWriter(fs.getConf(), opts);
			for (Object key : dataMap.keySet()) {
				Text temp = new Text();
				writer.append(keyClazz.getMethod("set", Object.class).invoke(key), valClazz.getMethod("set", Object.class).invoke(dataMap.get(key)));
			}
		} catch (Exception e) {
			logger.error("写入sequence file 发生异常！", e);
			return false;
		} finally {
			IOUtils.closeStream(writer);
		}
		return true;
	}
	
	/**
	 * 读取sequence file
	 * @param filePath 读取文件地址
	 * @return 
	 */
	@SuppressWarnings("null")
	public Map<Object, Object> readSeqFile(String filePath) {
		
		Map<Object, Object> resultMap = null;
		
		SequenceFile.Reader reader = null;
		Reader.Option[] opts = {Reader.file(new Path(filePath))};
		
		try {
			reader = new SequenceFile.Reader(fs.getConf(), opts);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());

			while (reader.next(key, value)) {
				resultMap.put((Object) key, (Object) value);
			}
		} catch (Exception e) {
			logger.error("读取sequence file 发生异常！", e);
			return null;
		} finally {
			IOUtils.closeStream(reader);
		}
		return resultMap;
	}
	
	/**
	 * java 类型映射 writable 类型
	 * @param clazz
	 * @return
	 */
	public Class<? extends Writable> mappingWritableClazz(Class<? extends Object> clazz) {
		
		Class<? extends Writable> writableClazz = null;
		
		if (clazz == String.class) {
			writableClazz = Text.class;
		} else if (clazz == Long.class) {
			writableClazz = VLongWritable.class;
		} else if (clazz == Integer.class) {
			writableClazz = IntWritable.class;
		} else if (clazz == Short.class) {
			writableClazz = ShortWritable.class;
		} else if (clazz == Double.class) {
			writableClazz = DoubleWritable.class;
		} else if (clazz == Float.class) {
			writableClazz = FloatWritable.class;
		} else if (clazz == Byte.class) {
			writableClazz = ByteWritable.class;
		} else if (clazz == Boolean.class) {
			writableClazz = BooleanWritable.class;
		} else {
			writableClazz = ObjectWritable.class;
		}
		
		return writableClazz;
	}
	
	/**
	 * 将具有原始类型的类转为原始类型，以适应writable子类的构造方法
	 * @param clazz
	 * @return
	 */
	public Class<? extends Object> transferToPrimitive(Class<? extends Object> clazz) {
		
		Class<? extends Object> primitive = null;
		
		if (clazz == Boolean.class) {
			primitive = boolean.class;
		} else if (clazz == Long.class) {
			primitive = long.class;
		} else if (clazz == Integer.class) {
			primitive = int.class;
		} else if (clazz == Short.class) {
			primitive = short.class;
		} else if (clazz == Double.class) {
			primitive = double.class;
		} else if (clazz == Float.class) {
			primitive = float.class;
		} else if (clazz == Byte.class) {
			primitive = byte.class;
		} else {
			primitive = clazz;
		}
		
		return primitive;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
//		HdfsSequenceUtil seq = new HdfsSequenceUtil();
//		Map<Integer, String> dataMap = new HashMap<Integer, String>();
//		dataMap.put(1, "test1");
//		dataMap.put(2, "test2");
//		seq.test(Text.class);
	}

}
