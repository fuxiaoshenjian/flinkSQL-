package utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;
import com.alibaba.fastjson.*;


public class MetadataLoader {
	private static Properties properties;
	public static Map<String, Map> graph;
	public static Map<String, Object> map;
	public static Map<String, Object> table;
	static{
		FileInputStream in = null;
		try{
			properties = new Properties();
			graph = JSON.parseObject(read("metadata/graph.json"), Map.class);
			map = JSON.parseObject(read("metadata/map.json"),Map.class);
			table = JSON.parseObject(read("metadata/table.json"),Map.class);
			System.out.println("读取元数据成功！");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("读取元数据失败！");
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
	public static void main(String[] args){
		//System.out.println(dw0);
	}

	public static String read(String path) throws Exception{
		InputStream is = MetadataLoader.class.getClassLoader().getResourceAsStream(path);

		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		int tempbyte;
		StringBuffer sb = new StringBuffer();
		while ((tempbyte = br.read()) != -1) {
			if (((char) tempbyte) != '\r') {
				sb.append((char) tempbyte);
			}
		}return sb.toString();
	}
}
