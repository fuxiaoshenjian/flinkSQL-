package com.dcits.flinksql.examples;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class DoubleStreamJoin {
    public static void main(String[] args) throws Exception {
        // 获取所需要的端口号
        int port = 9001,port1=9000;
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		env.setParallelism(3);
        String hostname = "127.0.0.1";
        String delimiter = "\n";
        // 链接socket获取输入的数据

        DataStreamSource<String> inputb = env.socketTextStream(hostname, port1, delimiter);
        DataStreamSource<String> inputa = env.socketTextStream(hostname, port, delimiter);
        
        TypeInformation[] fieldTypes =new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,};
		//TypeInformation[] fieldTypes=null;
		RowTypeInfo rowTypeInfo =new RowTypeInfo(fieldTypes);
		//Row r = new Row()
		DataStream<Row> tablea = inputa.map(x->toOrder(x)).returns(new RowTypeInfo(fieldTypes));;
        DataStream<Row> tableb = inputb.map(x->toOrder(x)).returns(new RowTypeInfo(fieldTypes));
		tableEnv.registerDataStream("tablea", tablea, "users, product, amount");
		tableEnv.registerDataStream("tableb", tableb, "users, product, amount");

        Table result = tableEnv.sqlQuery("SELECT * FROM tablea JOIN "
        		+ "tableb on tablea.amount = tableb.amount");
        
        Table result1 = tableEnv.sqlQuery("SELECT count(tablea.users,tableb.product,tablea.amount) FROM tablea JOIN "
        		+ "tableb on tablea.amount = tableb.amount");
        //tableEnv.toRetractStream(result1, Long.class).print();
        tableEnv.toAppendStream(result, Row.class).print();
     // execute
        try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
    
    public static Row toOrder(String value){
    	String[] values = value.split(",");
    	Row row = new Row(values.length);
		for (int i = 0; i < values.length; i++) {
			row.setField(i, values[i]);
		}
    	return row;
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