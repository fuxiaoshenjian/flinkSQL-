package utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;


public class PropertyUtil{
	private static final String PROPERTIES_NAME = "src/resources/base.properties";
	public static String DB_DRIVER = null;
	public static String DB_URL = null;
	public static String DB_USER = null;
	public static String DB_PWD = null;
	private static Properties properties;
	static{
		FileInputStream in = null;
		try{
			properties = new Properties();
			
			InputStream ips;
			InputStream is=PropertyUtil.class.getClassLoader().getResourceAsStream("base.properties");  
			BufferedReader br=new BufferedReader(new InputStreamReader(is));  
			
			properties.load(br);//jar包中
			System.out.println("读取配置信息成功！");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("读取配置信息失败！");
		}finally{
			if(in != null){
				try{
					in.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}
	}
	
	public static String get(String key){
		return properties.getProperty(key);
	}
	
	public static String[] getTopics(){
		return get("topic").split(",");
	}
	public static void main(String[] args){
		System.out.println(get("sparkMaster"));
	}

	public static String getConf(String string) {
		// TODO Auto-generated method stub
		return get(string);
	}
}
