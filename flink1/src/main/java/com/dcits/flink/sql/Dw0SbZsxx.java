package com.dcits.flink.sql;

import com.dcits.flink.io.DjNsrIO;
import com.dcits.flink.io.SbZsxxIO;
import com.dcits.flink.model.*;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.commons.logging.Log;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Dw0SbZsxx {
	private final static Log log = LogFactory.getLog(Dw0SbZsxx.class);
	static DataStreamSource<String> inputa, inputb;
	static String table1, table2;
	static StreamExecutionEnvironment env;
	static StreamTableEnvironment tableEnv;
	private static final String KAFKASERVER = "10.0.17.66:6667";
	private static final String KAFKATOPIC = "djnsrxx";

	static{
		readInput();
	}

	public static void main(String[] args) throws Exception {
		run();
    }

    private static void run(){
		DataStream<DjNsrxx> tablea = inputa.map(x->new DjNsrxx(x));
		DataStream<SbZsxx> tableb = inputb.map(x->new SbZsxx(x));

		tableEnv.registerDataStream(table1+"_", tablea, DjNsrxx.getFields());
		tableEnv.registerDataStream(table2+"_", tableb, SbZsxx.getFields());

		//TODO table转stream，在此处优化IO
		Table x = tableEnv.sqlQuery("SELECT * FROM ods_sb_zsxx_ a WHERE not exists " +
				"( SELECT * FROM ods_dj_nsrxx_ b WHERE b.djxh = a.djxh )");
		DataStream<Tuple2<Boolean, SbZsxx>> tableb_  = tableEnv.toRetractStream(x, SbZsxx.class);
		Table y = tableEnv.sqlQuery("SELECT * FROM ods_dj_nsrxx_ a WHERE not exists " +
				"( SELECT * FROM ods_sb_zsxx_ b WHERE b.djxh = a.djxh )");
		DataStream<Tuple2<Boolean, DjNsrxx>> tablea_  = tableEnv.toRetractStream(y, DjNsrxx.class);

		DataStream<SbZsxx> SbZsxx_Ywzl = getSbxxYwzl(tablea_);
		DataStream<DjNsrxx> DjNsrxx_Ywzl = getNsrYwzl(tableb_);

		DataStream<DjNsrxx> djnsr = tablea.union(DjNsrxx_Ywzl);
		DataStream<SbZsxx> sbzsxx = tableb.union(SbZsxx_Ywzl);
		/**注册表******************/
		tableEnv.registerDataStream(table1, djnsr, DjNsrxx.getFields());
		tableEnv.registerDataStream(table2, sbzsxx, SbZsxx.getFields());
		tableEnv.registerDataStream("dj_nsrxx_ywzl_qxsj", DjNsrxx_Ywzl, "nsrsbh,djxh,IDF_ETLSJJZRQ,IDF_FLAG,IDF_XH");
		tableEnv.registerDataStream("sb_zsxx_ywzl_qxsj", SbZsxx_Ywzl, SbZsxx.getFields());
		/**注册表******************/

		//select出最新版本的数据
		//TODO SELECT+注册的逻辑，统一封装一下
		Table dw_sb_zsxx = tableEnv.sqlQuery("select * from " +
				"ods_sb_zsxx a where a.IDF_XH=(select max(b.IDF_XH) from ods_sb_zsxx b" +
				" where 1=1 and a.djxh= b.djxh and a.IDF_FLAG='I' "+
					" )");
		tableEnv.registerTable("dw_sb_zsxx", dw_sb_zsxx);
		Table dw_dj_nsrxx = tableEnv.sqlQuery("select * from " +
				"ods_dj_nsrxx a where a.IDF_XH=(select max(b.IDF_XH) from ods_dj_nsrxx b" +
				" where 1=1 and a.djxh= b.djxh and a.IDF_FLAG='I' "+
				" )");
		tableEnv.registerTable("dw_dj_nsrxx", dw_dj_nsrxx);
		Table add = tableEnv.sqlQuery("select y.nsrsbh  , x.djxh  , x.IDF_XH  , x.zsxh  ,x.zsxm_dm from " +
				"(select * from ods_sb_zsxx a where a.IDF_XH=(select max(b.IDF_XH) from ods_sb_zsxx b where 1=1 and a.djxh= b.djxh and a.IDF_FLAG='I')) x " +
				"join (select * from ods_dj_nsrxx a where a.IDF_XH=(select max(b.IDF_XH) from ods_dj_nsrxx b where 1=1 and a.djxh= b.djxh and a.IDF_FLAG='I')) y  on x.djxh = y.djxh");

		Table newnsr = tableEnv.sqlQuery("select * from  ods_sb_zsxx a where a.IDF_XH=(select max(b.IDF_XH) " +
			"from ods_sb_zsxx b where 1=1 and a.djxh= b.djxh and a.IDF_FLAG='I')");
		Table newsb = tableEnv.sqlQuery("select * from  ods_dj_nsrxx a where a.IDF_XH=(select max(b.IDF_XH) " +
				"from ods_dj_nsrxx b where 1=1 and a.djxh= b.djxh and a.IDF_FLAG='I')");
		tableEnv.toRetractStream(newnsr, Row.class).print();
		tableEnv.toRetractStream(newsb, Row.class).print();
		Table delete =  tableEnv.sqlQuery("select b.nsrsbh, a.djxh, a.se, a.zsxh, a.zsxm_dm " +
				"from sb_zsxx_ywzl_qxsj a join " +
				"dj_nsrxx_ywzl_qxsj b on 1=1 and( a.djxh = b.djxh ) ");
		//TableSink mySink = new MySink();
		tableEnv.toRetractStream(add, Row.class).print();//.addSink(new MySqlSink());//.addSink();//writeToSocket(new MySink());//.addSink((new MySink());
		//tableEnv.toRetractStream(delete, Row.class).print();//.addSink(new MySqlSink());//.addSink();//writeToSocket(new MySink());//.addSink((new MySink());
		tableEnv.toRetractStream(add, Row.class).addSink(new DbSink("dw0_sb_zsxx", "nsrsbh , djxh , se , zsxh , zsxm_dm,IDF_FLAG"));
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
	public static void readInput(){
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		table2 = "ods_sb_zsxx";
		table1 = "ods_dj_nsrxx";
		tableEnv = TableEnvironment.getTableEnvironment(env);
		env.setParallelism(10);
		env = StreamExecutionEnvironment.getExecutionEnvironment();  Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", KAFKASERVER);
		properties.setProperty("zookeeper.connect", "10.0.17.66:2181");
		properties.setProperty("group.id", "test");
		StreamQueryConfig qConfig = tableEnv.queryConfig();
		qConfig.withIdleStateRetentionTime(Time.hours(12), Time.hours(16));
		FlinkKafkaConsumer<String> consumerA =
				new FlinkKafkaConsumer(KAFKATOPIC, new SimpleStringSchema(), properties);

		FlinkKafkaConsumer<String> consumerB =
				new FlinkKafkaConsumer("sbxx", new SimpleStringSchema(), properties);
		inputa =  env.addSource(consumerA);
		inputb =  env.addSource(consumerB);
	}

	/**
	 * 根据nsr流中的记录，取出对应sbxx中的历史记录，作为Sbxx历史流
	 * @param source
	 * @return
	 */
	public static DataStream<SbZsxx> getSbxxYwzl(DataStream<Tuple2<Boolean, DjNsrxx>>source){

		DataStream<SbZsxx> sbzxx =  AsyncDataStream.
				unorderedWait(source, new SbZsxxIO(), 30000, TimeUnit.MILLISECONDS, 100).
				flatMap(new FlatMapFunction<List<SbZsxx>, SbZsxx>() {
					@Override
					public void flatMap(List<SbZsxx> s, Collector<SbZsxx> collector) throws Exception {
						if(s==null){
							return;
						}
						for (SbZsxx token : s) {
							collector.collect(token);
						}
					}
				});
		return sbzxx;
	}

	/**
	 * 根据Sbxx流中的记录，取出对应Nsr中的历史记录，作为Nsr历史流
	 * @param source
	 * @return
	 */
	public static DataStream<DjNsrxx> getNsrYwzl(DataStream<Tuple2<Boolean,SbZsxx>> source){
		DataStream<DjNsrxx> nsrxx = AsyncDataStream.
				unorderedWait(source, new DjNsrIO(), 30000, TimeUnit.MILLISECONDS, 100).
				flatMap(new FlatMapFunction<List<DjNsrxx>, DjNsrxx>() {
					@Override
					public void flatMap(List<DjNsrxx> s, Collector<DjNsrxx> collector) throws Exception {
						if(s==null){
							return;
						}
						for (DjNsrxx token : s) {
								collector.collect(token);
						}
					}
				});
		return nsrxx;
	}

}