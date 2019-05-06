package examples.async;

import java.sql.ResultSet;
import java.util.concurrent.Callable;

import utils.MySqlUtil;

public class ComputeTask implements Callable<String> {  
	  
    private int user = 0;  
    
    public ComputeTask(Integer amount){  
        //System.out.println("生成子线程计算任务: "+taskName);  
    	this.user = amount;
    }  
    
    @Override  
    public String call() throws Exception {  
        // TODO Auto-generated method stub  
        int amount = 0;
    	MySqlUtil mysql = new MySqlUtil();
    	ResultSet rs = mysql.executeQuery("select * from orders where user = "+user);
    	rs.last();
        //获取当前行编号
        int num=rs.getRow();
        for(int i=1;i<=num;i++){
            rs.absolute(i);
            amount = rs.getInt("amount");
        }
        rs.close();
        return "amount:"+amount;  
    }  
}  