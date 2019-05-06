package examples.async;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.function.Supplier;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public class AsyncDatabaseRequest extends RichAsyncFunction<String, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void asyncInvoke(String key, ResultFuture<String> resultFuture) throws Exception {
		// TODO Auto-generated method stub
		int i = Integer.parseInt(key.split(",")[0]);
		CompletableFuture.supplyAsync(new Supplier<String>() {
			@Override
	        public String get() {
	        	FutureTaskForMultiCompute inst=new FutureTaskForMultiCompute();    
	            ExecutorService exec = Executors.newFixedThreadPool(50);  
	            FutureTask<String> ft = new FutureTask<String>(new ComputeTask(i));   
	            exec.submit(ft);  
	            try {
	          		System.out.println(ft.get());
	          	} catch (InterruptedException e) {
	          		// TODO Auto-generated catch block
	          		e.printStackTrace();
	          	} catch (ExecutionException e) {
	          		// TODO Auto-generated catch block
	          		e.printStackTrace();
	          	}
	            // 关闭线程池  
	            exec.shutdown();  
	            try {
	            	return ft.get();
				} catch (Exception e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
					return "";
				} 
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
