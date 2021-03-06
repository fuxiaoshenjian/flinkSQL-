package com.dcits.flinksql.examples;

import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

import org.apache.flink.table.sinks.*;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import examples.async.AsyncDatabaseRequest;
import utils.MySqlUtil;


public class DoubleStreamJoin {
    public static void main(String[] args) throws Exception {
        // 获取所需要的端口号
        int port = 9001,port1=9000;
        // 获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		env.setParallelism(3);
		StreamQueryConfig qConfig = tableEnv.queryConfig();
		qConfig.withIdleStateRetentionTime(Time.minutes(1), Time.minutes(6));
		String hostname = "127.0.0.1";
        String delimiter = "\n";
        // 链接socket获取输入的数据
        DataStreamSource<String> inputb = env.socketTextStream(hostname, port1, delimiter);
        DataStreamSource<String> inputa = env.socketTextStream(hostname, port, delimiter);
        TypeInformation[] fieldTypes =new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,};
		//TypeInformation[] fieldTypes=null;
		RowTypeInfo rowTypeInfo =new RowTypeInfo(fieldTypes);
		//Row r = new Row()
		
		DataStream<Row> tablea = inputa.map(x->toRow(x)).returns(new RowTypeInfo(fieldTypes));;
        
		DataStream<Row> tableb = inputb.map(x->toRow(x)).returns(new RowTypeInfo(fieldTypes));
		tableEnv.registerDataStream("tablea", tablea, "users, product, amount");
		tableEnv.registerDataStream("tableb", tableb, "users, product, amount");
		
		Table tablea1 = tableEnv.sqlQuery("SELECT * FROM tablea");
		Table tableb1 = tableEnv.sqlQuery("SELECT * FROM tableb");
		tableEnv.registerTable("tablea1", tableb1);
		tableEnv.registerTable("tableb1", tablea1);

//		tableEnv.toRetractStream(tablea1, Row.class).addSink(new SbZsxxSink());
//		tableEnv.toRetractStream(tableb1, Row.class).addSink(new SbZsxxSink());
        Table result = tableEnv.sqlQuery("SELECT * FROM tablea1 JOIN "
        		+ "tableb1 on tablea1.amount = tableb1.amount");
        
       tableEnv.registerTable("results", result);
       Table count = tableEnv.sqlQuery("select count(*) from results");
       Table result1 = tableEnv.sqlQuery("SELECT tablea.users, count(tablea.users,tableb.product,tablea.amount) "
        		+ "FROM tablea "
        		+ "JOIN "
        		+ "tableb on tablea.amount = tableb.amount "
        		+ "group by tablea.users");

        tableEnv.toRetractStream(count, Long.class).print();
        //tableEnv.toAppendStream(result, Row.class).writeAsText("d:\\wangydh\\join");//.print();
        //tableEnv.toRetractStream(result, Row.class).print();
     // execute
        try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
    
    public static Row toRow(String value){
    	//ResultSet rs = mysql.executeQuery("select * from orders where user = "+1);
//    	int amount = 0;
//    	try{
//	    	rs.last();
//	        //获取当前行编号
//	        int num=rs.getRow();
//	        for(int i=1;i<=num;i++){
//	            rs.absolute(i);
//	            amount = rs.getInt("amount");
//	        }
//    	}catch (Exception e){
//    		;
//    	}
    	String[] values = value.split(",");
    	Row row = new Row(values.length);
    	row.setField(0, Integer.parseInt(values[0]));
    	row.setField(1, values[1]);
    	row.setField(2, Integer.parseInt(values[2]));
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