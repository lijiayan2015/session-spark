package com.ljy.sessionanalyze.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * 
 * 提供外界获取某个配置key对应的value的方法
 * @author Administrator
 */
public class ConfigurationManager {
	
	// private来修饰，避免外界访问来更改值
	private static Properties prop = new Properties();
	
	/**
	 * 静态代码块
	 * 静态块用于初始化类，为类的属性初始化
	 * 每个静态代码块只会执行一次
	 */
	static {
		try {
			// 通过调用类加载器（ClassLoader）的getResourceAsStream方法获取指定文件的输入流
			InputStream in = ConfigurationManager.class
					.getClassLoader().getResourceAsStream("my.properties");
			prop.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取指定key对应的value
	 * @param key 
	 * @return value
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置项
	 * @param key
	 * @return value
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取布尔类型的配置项
	 * @param key
	 * @return value
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 获取Long类型的配置项
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}
	
}
