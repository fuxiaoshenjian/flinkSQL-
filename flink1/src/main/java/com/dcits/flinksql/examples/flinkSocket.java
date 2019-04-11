package com.dcits.flinksql.examples;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import com.dcits.flinksql.examples.StreamSQLExample.Order;

public class flinkSocket {
    public static void main(String[] args) throws Exception {
        // 获取所需要的端口号
        int port = 9001;
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
	   
        String hostname = "127.0.0.1";
        String delimiter = "\n";
        // 链接socket获取输入的数据
        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
				new Order(2L, "pen", 3),
				new Order(2L, "rubber", 2),
				new Order(4L, "beer", 1)));
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);
        DataStream<Order> tablea = text.map(x->toOrder(x));
		tableEnv.registerDataStream("tableb", orderB, "users, product, amount");
		tableEnv.registerDataStream("tablea", tablea, "users, product, amount");
        //Table table = tableEnv.fromDataStream(ds, "user");
//        Table result = tableEnv.sqlQuery("SELECT * FROM tablea WHERE amount > 2 UNION ALL " +
//				"SELECT * FROM tableb WHERE amount < 2");
        Table result = tableEnv.sqlQuery("SELECT tablea.users,tableb.product,tablea.amount FROM tablea JOIN "
        		+ "tableb on tablea.amount = tableb.amount");
             tableEnv.toAppendStream(result, Order.class).print();
     // execute
        try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
    
    public static Order toOrder(String value){
    	String[] x = value.split(",");
    	long users =  Long.parseLong(x[0]);
    	int amount = Integer.parseInt(x[2]);
    	return new Order(users,x[1],amount);
    }

    public static class WordIsCount{
        public String word;
        public long count;

        public WordIsCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public WordIsCount() {
        }

        @Override
        public String toString() {
            return "WordIsCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}