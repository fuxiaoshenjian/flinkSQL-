package examples.async;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import utils.PropertyUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class AsyncConnectionPool {

    private ConcurrentHashMap<String, FutureTask<Connection>> pool = 
    		new ConcurrentHashMap<String, FutureTask<Connection>>();
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    public Connection getConnection(String key) throws InterruptedException, ExecutionException {
        FutureTask<Connection> connectionTask = pool.get(key);
        if (connectionTask != null) {
            return connectionTask.get();
        } else {
           
            //有可能多个线程同时进入从而创建多个callable，但木有关系
            Callable<Connection> callable = new Callable<Connection>() {
                @Override
                public Connection call() throws Exception {
                    return createConnection();
                }
            };
            FutureTask<Connection> newTask = new FutureTask<Connection>(callable);

			//因为使用的ConcurrentHashMap，所以这里最终只会put成功有一个线程的数据。当put成功之后会返回v。如果返回的v是null，
            connectionTask = pool.putIfAbsent(key, newTask);
            if (connectionTask == null) {
                connectionTask = newTask;
                connectionTask.run();
            }
            return connectionTask.get();
        }
    }

    public Connection createConnection() {
    	String DB_URL = PropertyUtil.get("DB_URL").endsWith("/")?PropertyUtil.get("DB_URL")+"iotmp":PropertyUtil.get("DB_URL")+"/iotmp";//"jdbc:mysql://10.126.3.180:3306/iotmp";
    	String DB_URL_IOT = PropertyUtil.get("DB_URL").endsWith("/")?PropertyUtil.get("DB_URL")+"dw_iot":PropertyUtil.get("DB_URL")+"/dw_iot";
    	String USER = PropertyUtil.get("DB_USER");
    	String PASS = PropertyUtil.get("DB_PSW");
        Connection conn = null;
		try {
			conn = DriverManager.getConnection(
					DB_URL,
					USER,
					PASS
			);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}return conn;
    }

}