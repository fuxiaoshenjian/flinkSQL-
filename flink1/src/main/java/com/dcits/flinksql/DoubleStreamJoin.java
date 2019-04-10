package com.dcits.flinksql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import com.dcits.flinksql.StreamSQLExample.Order;

public class DoubleStreamJoin {
    public static void main(String[] args) throws Exception {
        // 获取所需要的端口号
        int port = 9001,port1=9000;
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
	   
        String hostname = "127.0.0.1";
        String delimiter = "\n";
        // 链接socket获取输入的数据

        DataStreamSource<String> inputb = env.socketTextStream(hostname, port1, delimiter);
        DataStreamSource<String> inputa = env.socketTextStream(hostname, port, delimiter);
        DataStream<Order> tablea = inputa.map(x->toOrder(x));
        DataStream<Order> tableb = inputb.map(x->toOrder(x));
		tableEnv.registerDataStream("tableb", tableb, "users, product, amount");

		tableEnv.registerDataStream("tablea", tablea, "users, product, amount");
		//Table tablea1 = tableEnv.sqlQuery("select * from tablea");
		//tableEnv.toAppendStream(tablea1, Order.class);

        Table result = tableEnv.sqlQuery("SELECT tablea.users,tableb.product,tablea.amount FROM tablea JOIN "
        		+ "tableb on tablea.amount = tableb.amount");
        
        //.window(TumblingEventTimeWindows.of(Time.seconds(1L)));
        Table result1 = tableEnv.sqlQuery("SELECT count(tablea.users,tableb.product,tablea.amount) FROM tablea JOIN "
        		+ "tableb on tablea.amount = tableb.amount");
        tableEnv.toRetractStream(result1, Long.class).print();
        //tableEnv.toAppendStream(result1, Integer.class).print();
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
    	return new Order(users, x[1], amount);
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