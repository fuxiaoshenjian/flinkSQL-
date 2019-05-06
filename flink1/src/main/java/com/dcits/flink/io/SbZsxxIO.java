package com.dcits.flink.io;

import com.dcits.flink.model.DjNsrxx;
import com.dcits.flink.model.SbZsxx;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.function.Supplier;

public class SbZsxxIO extends RichAsyncFunction<Tuple2<Boolean, DjNsrxx>, List<SbZsxx>>{

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void asyncInvoke(Tuple2<Boolean, DjNsrxx> key, ResultFuture<List<SbZsxx>> resultFuture) throws Exception {
		// TODO Auto-generated method stub
		CompletableFuture.supplyAsync(new Supplier<List>() {
			@Override
	        public List get() {
				if(key.f0==false){
					return null;
				}
	            ExecutorService exec = Executors.newFixedThreadPool(50);
	            FutureTask<List<SbZsxx>> ft = new FutureTask<>(new SbZsxxComputeTask(key.f1.djxh));
	            exec.submit(ft);
	            // 关闭线程池  
	            exec.shutdown();  
	            try {
	            	return ft.get();
				} catch (Exception e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				} 
	        }  
        }).thenAccept( (List rs) -> {
            try {
				resultFuture.complete(Collections.singleton(rs));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        });
	}

}
