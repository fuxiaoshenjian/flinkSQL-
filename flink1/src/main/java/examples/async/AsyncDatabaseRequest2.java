package examples.async;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.Supplier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import utils.MysqlUtil;

public class AsyncDatabaseRequest2 extends RichAsyncFunction<String, String>{

    
	@Override
	public void asyncInvoke(String key, ResultFuture<String> resultFuture) throws Exception {
		// TODO Auto-generated method stub
		int i = Integer.parseInt(key.split(",")[2]);
		Future<String> ft = new FutureTask<String>(new ComputeTask(3));
		CompletableFuture.supplyAsync(new Supplier<String>() {

            @Override
            public String get() {
             	MysqlUtil mysql = new MysqlUtil();
            	ResultSet rs = mysql.executeQuery("select * from orders where amount = "+i);
            	int users=0;
            	try{
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
            	}
            	catch (Exception e){
            		return "fuck";
            	}
                return "user:"+users;
            }
        }).thenAccept( (String dbResult) -> {
            try {
				resultFuture.complete(Collections.singleton(dbResult));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        });
	}

}
