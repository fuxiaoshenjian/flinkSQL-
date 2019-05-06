package com.dcits.flink.sql;

import com.dcits.flink.io.CommonIO;
import com.dcits.flink.io.DjNsrIO;
import com.dcits.flink.io.SbZsxxIO;
import com.dcits.flink.model.DjNsrxx;
import com.dcits.flink.model.SbZsxx;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
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
import utils.KafkaUtil;
import utils.MetadataLoader;
import utils.TypeUtil;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 双流join操作的通用Class
 */
public class DoubleJoin {
	private final Log log = LogFactory.getLog(DoubleJoin.class);
	DataStreamSource<String> input1, input2;
	String table1, table2;
	StreamExecutionEnvironment env;
	StreamTableEnvironment tableEnv;
    Properties properties;
    Map meta;

    public DoubleJoin(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, Map meta,
                      Map<String, DataStream> streamMap){
        env = env;
        tableEnv = tableEnv;
        properties = KafkaUtil.getProperties();
        meta = meta;
    }

    public void run(){
        int type = readInput();
        if(type == 0){
            KjK();
        }else if(type == 1){

        }

    }
    /**
     * table1的映射。现在默认value中不包含表示table的属性，如果包含了，一个函数就可以了。
     * @param value
     * @return
     */
    public Row toRow(String value){
        Map<String,String> meta1 = (Map) MetadataLoader.table.get(table1);
        Row row = new Row(meta1.size());
        int pos=0;
        for(String key1:meta1.keySet()){
            TypeUtil.getStringRow(row, meta1.get(key1), value, pos);
            pos++;
        }
        return row;
    }

    /**
     * table2的映射。现在默认value中不包含表示table的属性，如果包含了，一个函数就可以了。
     * @param value
     * @return
     */
    public Row toRow1(String value){
        Map<String,String> meta1 = (Map) MetadataLoader.table.get(table2);
        Row row = new Row(meta1.size());
        int pos=0;
        for(String key1:meta1.keySet()){
            TypeUtil.getStringRow(row, meta1.get(key1), value, pos);
            pos++;
        }
        return row;
    }

    /**
     * 已注册表 join 已注册表
     */
    public void TjT(){

    }
    /**
     * kafka 批量 join kafka 全量情景
     */
    public void KjK(){
        readInput();
        TypeInformation[] fieldTypes1,fieldTypes2;
        fieldTypes1 = TypeUtil.getTypeInfo(table1);
		DataStream<Row> tablea = input1.map(x->toRow(x)).returns(new RowTypeInfo(fieldTypes1));
        fieldTypes2 = TypeUtil.getTypeInfo(table2);
		DataStream<Row> tableb = input2.map(x->toRow1(x)).returns(new RowTypeInfo(fieldTypes2));

		tableEnv.registerDataStream(table1+"_", tablea, TypeUtil.allTypes(table1));
		tableEnv.registerDataStream(table2+"_", tableb, TypeUtil.allTypes(table2));

		//查询对应流中不存在的记录
		Table x = tableEnv.sqlQuery("SELECT * FROM ods_sb_zsxx_ a WHERE not exists " +
				"( SELECT * FROM ods_dj_nsrxx_ b WHERE b.djxh = a.djxh )");
		DataStream<Tuple2<Boolean, Row>> tableb_  = tableEnv.toRetractStream(x, Row.class);
		Table y = tableEnv.sqlQuery("SELECT * FROM ods_dj_nsrxx_ a WHERE not exists " +
				"( SELECT * FROM ods_sb_zsxx_ b WHERE b.djxh = a.djxh )");
		DataStream<Tuple2<Boolean, Row>> tablea_  = tableEnv.toRetractStream(y, Row.class);

		DataStream<Row> SbZsxx_Ywzl = getSbxxYwzl(tablea_, table1);
		DataStream<Row> DjNsrxx_Ywzl = getSbxxYwzl(tableb_, table2);

		DataStream<Row> djnsr = tablea.union(DjNsrxx_Ywzl);
		DataStream<Row> sbzsxx = tableb.union(SbZsxx_Ywzl);
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
	 * TODO 改写。获取输入流，kafka、socket等多种方式.
     * 此处默认topic和注册的表同名。
     * @return 0：双kafka流 1：双table已注册，直接执行sql即可。
     * 2：table1 已经注册。 3：table2 已经注册
     */
	public int readInput(){
	    List<Map> inputs = (List) meta.get("input");
	    /**JOIN操作，默认配置正确包含两项输入*/
	    Map<String, String>  m1 = inputs.get(0),m2=inputs.get(1);
	    String _type1 = (String)m1.get("type");String _type2 = (String)m2.get("type");
	    table1 = (String) (m1.get("topic"));table2 = (String) m2.get("topic");
	    if(_type1.equalsIgnoreCase("kafka")){
	        FlinkKafkaConsumer<String> consumerA = new FlinkKafkaConsumer(table1, new SimpleStringSchema(),
                    properties);
            input1 =  env.addSource(consumerA);
	    }
        if(_type2.equalsIgnoreCase("kafka")){
            FlinkKafkaConsumer<String> consumerB = new FlinkKafkaConsumer(table2, new SimpleStringSchema(),
                    properties);
            input2 =  env.addSource(consumerB);
        }
	    /**如果为table类型，不许要输入。注册完事儿即可*/
        if(_type1.equalsIgnoreCase("kafka") && _type1.equalsIgnoreCase("kafka")){
            return 0;
        }else if(!_type1.equalsIgnoreCase("kafka") && !_type1.equalsIgnoreCase("kafka")){
            return 1;
        }else if(!_type1.equalsIgnoreCase("kafka") && _type1.equalsIgnoreCase("kafka")){
            return 2;
        }else{
            return 3;
        }
	}

	/**
	 * 根据nsr流中的记录，取出对应sbxx中的历史记录，作为Sbxx历史流
	 * @param source
	 * @return
	 */
	public static DataStream<Row> getSbxxYwzl(DataStream<Tuple2<Boolean, Row>> source, String  currentTable){

		DataStream<Row> sbzxx =  AsyncDataStream.
				unorderedWait(source, new CommonIO(currentTable), 30000, TimeUnit.MILLISECONDS, 100).
				flatMap(new FlatMapFunction<List<Row>, Row>() {
					@Override
					public void flatMap(List<Row> s, Collector<Row> collector) throws Exception {
						if(s==null){
							return;
						}
						for (Row token : s) {
							collector.collect(token);
						}
					}
				});
		return sbzxx;
	}

}