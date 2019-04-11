package examples.async;

import java.sql.ResultSet;
import java.util.concurrent.Callable;

import utils.MysqlUtil;

public class ComputeTask implements Callable<String> {  
	  
    private int amount = 0;  
    
    public ComputeTask(Integer amount){  
        //System.out.println("生成子线程计算任务: "+taskName);  
    	this.amount = amount;
    }  
    
    @Override  
    public String call() throws Exception {  
        // TODO Auto-generated method stub  
        int users = 0;
        // 休眠5秒钟，观察主线程行为，预期的结果是主线程会继续执行，到要取得FutureTask的结果是等待直至完成。  
//        Thread.sleep(5000); 
    	MysqlUtil mysql = new MysqlUtil();
    	ResultSet rs = mysql.executeQuery("select * from orders where amount = "+amount);
    	rs.last();
        //获取当前行编号
        int num=rs.getRow();
        for(int i=1;i<=num;i++){
            //将光标移动到此ResultSet对象的给定行
            rs.absolute(i);
            //System.out.println(rs.getInt("amount")+" "+rs.getInt("user")+" "+rs.getString("product"));
            users = rs.getInt("user");
        }
        rs.close();

//        System.out.println("子线程计算任务: "+taskName+" 执行完成!");  
        return "user:"+users;  
    }  
}  