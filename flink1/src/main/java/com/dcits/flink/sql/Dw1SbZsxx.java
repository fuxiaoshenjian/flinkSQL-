package com.dcits.flink.sql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Dw1SbZsxx {
	private final static Log log = LogFactory.getLog(Dw1SbZsxx.class);
	static String dw0_sb_zsxx = "dw0_sb_zsxx";
	static Table table;
	static StreamExecutionEnvironment env;
	static StreamTableEnvironment tableEnv;

	public static void main(String[] args) throws Exception {
		run();
	}

	private static void run() {
		Table dw1_sb_zsxx = tableEnv.sqlQuery("select a.nsrsbh, sum(a.se) from ods.dw0_sb_zsxx a group by a.nsrsbh ");
		tableEnv.toRetractStream(dw1_sb_zsxx, Row.class).addSink(new DbSink("dw1_sb_zsxx",
				"nsrsbh,se, IDF_ETLSJJZRQ, IDF_FLAG"));
		try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * TODO 改写。获取输入流，kafka、socket等多种方式
	 */
	public static void readInput(Table table) {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		tableEnv = TableEnvironment.getTableEnvironment(env);
		env.setParallelism(10);
		env = StreamExecutionEnvironment.getExecutionEnvironment();
	}
}