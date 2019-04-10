package com.dcits.flinksql;
//import com.alibaba.fastjson.JSONObject;
//import com.enniu.cloud.services.riskbrain.flink.job.EnniuKafkaSource;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import com.dcits.flinksql.StreamSQLExample.Order;

/**
 * flinkSQL读入mysql。
 * @author wangydh
 *
 */
public class MysqlTableApiJob {
	
	public static void main(String[] args){
	
		TypeInformation[] fieldTypes =new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,};
		//TypeInformation[] fieldTypes=null;
		RowTypeInfo rowTypeInfo =new RowTypeInfo(fieldTypes);
		JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername("com.mysql.jdbc.Driver")
			.setDBUrl("jdbc:mysql://172.16.3.78/test")
			.setUsername("root")
			.setPassword("mysql")
			.setQuery("select user,product,amount from orders")
			.setRowTypeInfo(rowTypeInfo)
			.finish();
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Row> datasource = env.createInput(jdbcInputFormat);
		DataSet<Order> set = datasource.map(r->toOrder(r));
						//datasource

		BatchTableEnvironment tableEnv =new BatchTableEnvironment(env, TableConfig.DEFAULT());
		tableEnv.registerDataSet("t2", set, "users,product,amount");
		tableEnv.sqlQuery("select * from t2").printSchema();
		Table query = tableEnv.sqlQuery("select * from t2 where amount > 1");
		DataSet result = tableEnv.toDataSet(query, Row.class);
		try {
			result.print();
			System.out.println(result.count());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    public static Order toOrder(Row r){
    	long users =  Long.parseLong(r.getField(0)+"");
    	int amount = Integer.parseInt(r.getField(2)+"");
    	return new Order(users,r.getField(1)+"",amount);
    }
}
