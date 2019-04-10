package com.dcits.flinksql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import akka.japi.tuple.Tuple3;

import com.alibaba.fastjson.JSONObject;
import com.dcits.flinksql.StreamSQLExample.Order;


public class StreamSqlDemo {

	private static final String KAFKASERVER = "10.126.3.93:6667";
	private static final String KAFKATOPIC = "WEBSOCKET";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
	   
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	 
	        //{"id":"int","name":"string","score":"int","currentTimeStamp":"string"}
	        //kafka schema
	        String schema = "{\"id\":\"int\",\"name\":\"string\",\"score\":\"int\",\"currentTimeStamp\":\"long\"}";
//	        JSONObject jsonObject = JSONObject.parseObject(schema); 
	 
	        Properties properties = new Properties();
	        properties.setProperty("bootstrap.servers", KAFKASERVER);
	        // only required for Kafka 0.8
	        properties.setProperty("zookeeper.connect", "10.126.3.93:2181");
	        properties.setProperty("group.id", "test");
	        //DataStream<String> stream = env
	        //		.addSource(new FlinkKafkaConsumer08<>("topic", new SimpleStringSchema(), properties))
//	        DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(null, null);
	
	        FlinkKafkaConsumer<Map> myConsumer =
	        	    new FlinkKafkaConsumer<>(KAFKATOPIC, new SchemaUT(), properties);
	        	    
	        DataStream<Map> ds =  env.addSource(myConsumer);
			DataStream<Order> orderB = env.fromCollection(Arrays.asList(
					new Order(2L, "pen", 3),
					new Order(2L, "rubber", 3),
					new Order(4L, "beer", 1)));
				// register DataStream as Table
			tableEnv.registerDataStream("OrderB", orderB, "users, product, amount");
	        //Table table = tableEnv.fromDataStream(ds, "user");
	        Table tableA = tableEnv.fromDataStream(ds, "users, product, amount");
//	        Table result = tableEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
//					"SELECT * FROM OrderB WHERE amount < 2");
	       Table result = tableEnv.sqlQuery("select * from OrderB");
	        tableEnv.toAppendStream(result, Order.class).print();
	     // execute
	        try {
				env.execute();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}

}
