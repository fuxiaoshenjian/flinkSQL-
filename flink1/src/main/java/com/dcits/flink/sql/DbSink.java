package com.dcits.flink.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import utils.ConnectionPool;
import utils.MySqlUtil;
import utils.PropertyUtil;

import java.sql.Connection;

public class DbSink extends RichSinkFunction<Tuple2<Boolean, Row>> {

    private String table;
    private Connection connection;
    private String cols;

    public DbSink(String table, String cols){
        this.table = table;
        this.cols = cols;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        String DB_URL =
                PropertyUtil.get("DB_URL").endsWith("/")?PropertyUtil.get("DB_URL")+"ods":PropertyUtil.get("DB_URL")+"/ods";
        DB_URL+="?serverTimezone=GMT%2B8&useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&useSSL=false";
        String USER = PropertyUtil.get("DB_USER");
        String PASS = PropertyUtil.get("DB_PSW");
        connection  = ConnectionPool
                .getConnection(DB_URL, USER, PASS);
    }
 
    @Override
    public void close() throws Exception {
        if(connection!=null){
            ConnectionPool.returnConnection(connection);
        }
        super.close();
    }
 
    @Override
    public void invoke(Tuple2<Boolean,Row> value, Context context) throws Exception {
        //如果是false，则删除。如果为true，则新增。
        int size = value.f1.getArity();
        Object[] o = new Object[size+1];
        StringBuffer sb = new StringBuffer("insert into ").append(table).append(" ( "+cols+" )").
                append(" values (");
        for (int i = 0; i < size+1; i++) {
            if(i!=size) {
                o[i] = value.f1.getField(i);
                sb.append("?");
                sb.append(",");
            }else{
                if(value.f0==true){
                    o[i]="I";sb.append("?");
                }else{
                    o[i]="D";sb.append("?");
                }
            }
        }
        sb.append(")");
        MySqlUtil.executeSQL(sb.toString(), o, connection);

    }
}
