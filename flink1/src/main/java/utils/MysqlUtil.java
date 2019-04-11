package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import utils.PropertyUtil;

public class MysqlUtil {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
    public static String DB_URL; 

    public static String DB_URL_IOT;
    public static String USER;
    public static String PASS;
    
    static{
    	DB_URL = PropertyUtil.get("DB_URL").endsWith("/")?PropertyUtil.get("DB_URL")+"test":PropertyUtil.get("DB_URL")+"/test";//"jdbc:mysql://10.126.3.180:3306/iotmp";
	    USER = PropertyUtil.get("DB_USER");
	    PASS = PropertyUtil.get("DB_PSW");
    }
    
	public ResultSet executeQuery(String sql) {
		// TODO Auto-generated method stub
		MysqlUtil mysql = new MysqlUtil();
		Connection conn = ConnectionPool.getConnection(DB_URL, USER,PASS);
		return query(sql, conn);
        
	}

	public ResultSet query(String sql, Connection conn){
		long startTime=System.currentTimeMillis();
		if(conn==null){
			System.out.println("ERROR!conn为空，"+sql);
			return null;
		}
	    Statement stmt = null;
	    ResultSet rs = null;
	    try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}            
	    long time=System.currentTimeMillis() - startTime; 
	    System.out.println("查询完毕，执行"+sql+" 耗时"+time+"ms");
        return rs;
	    
	   
	}

    public static int executeSQL(String preparedSql, Object[] param) throws ClassNotFoundException {
        Connection conn = null;
        PreparedStatement pstmt = null;
        /* 处理SQL,执行SQL */
        try {
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
            pstmt = conn.prepareStatement(preparedSql); // 得到PrepaRredStatement对象
            if (param != null) {
                for (int i = 0; i < param.length; i++) {
                    pstmt.setObject(i + 1, param[i]); // 为预编译sql设置参数
                }
            }
        pstmt.execute(); // 执行SQL语句
        } catch (SQLException e) {
            e.printStackTrace(); // 处理SQLException异常
        } finally {
            try {
                closeAll(conn, pstmt, null);
            } catch (SQLException e) {    
                e.printStackTrace();
            }
        }
        return 0;
    }
    
    public static int executeSQL(String preparedSql, Object[] param, Connection conn) throws ClassNotFoundException {
        PreparedStatement pstmt = null;
        /* 处理SQL,执行SQL */
        try {
            pstmt = conn.prepareStatement(preparedSql); // 得到PrepaRredStatement对象
            if (param != null) {
                for (int i = 0; i < param.length; i++) {
                    pstmt.setObject(i + 1, param[i]); // 为预编译sql设置参数
                }
            }
        pstmt.execute(); // 执行SQL语句
        } catch (SQLException e) {
            e.printStackTrace(); // 处理SQLException异常
        } 
        return 0;
    }
    
    public static void closeAll(Connection conn,Statement stmt,ResultSet rs) throws SQLException {
        if(rs!=null) {
            rs.close();
        }
        if(stmt!=null) {
            stmt.close();
        }
        if(conn!=null) {
            conn.close();
        }
    }
}
