package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

public class ConnectionPool {
    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection(String jdbcUrl,String user,String psw) {
        try {
            if (connectionQueue == null || connectionQueue.size()==0) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0;i < 5;i ++) {
                    Connection conn = DriverManager.getConnection(
                            jdbcUrl,
                            user,
                            psw
                    );
                    connectionQueue.push(conn);
                    System.out.println("连接池中创建5条连接。 此时池中剩余连接个数为 ："+connectionQueue.size());
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}