package com.dcits.flinksql.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.flink.api.common.time.Time;

public class socket2 {
	public static void main(String[] args){
		ServerSocket server = null;
        // 创建一个端口为9000监听客户端请求的serversocket
        try {
            server = new ServerSocket(9000);
            System.out.println("服务端启动成功：服务端端口号为9000");
        } catch (IOException e) {
            // 如果连接不上，打印出错信息
            System.out.println("can not listen to:"+e);
        }
        run(server);
}
	
	public static void run(ServerSocket server){
		System.out.println("重新开始 监听！");
	      Socket serverSocket = null;
	        try {
	            // 使用accept()阻塞等待客户请求，有客户请求则产生一个Socket对象，并继续执行
	            serverSocket = server.accept();
	            // 有客户端连接
	            System.out.println("有个客户端连接："+serverSocket.getInetAddress()+":"+serverSocket.getPort());
	        } catch (IOException e) {
	            // 客户端请求异常
	            System.out.println(e);
	        }
	        String line;
	        // 通过Socket对象得到输出流，构造printwriter对象
	        try{
	        PrintWriter serverPrintWriter = new PrintWriter(serverSocket.getOutputStream());
	        // 如果输入bye，停止循环
	        int x = 1;
	        while (true){
	        	Thread.sleep(1000);
	            // 向客户端输出字符串
	            serverPrintWriter.println("1,2,"+x);
	            System.out.println("1,2,"+x);
	            x++;
	            // 刷新输出流
	            serverPrintWriter.flush();
	            // 在系统控制台上打印输入的内容；
	            //System.out.println("Server:"+"1,2,3");
	            // 继续输入然后重新读取字符串
	        }
		}catch (Exception e){
			run(server);
			System.out.println(e);
		}
	}
	public static String factory(String x){
		return x;
	}
}
