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
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        Connection conn = connectionQueue.poll();
        return conn;
    }

    public synchronized static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}