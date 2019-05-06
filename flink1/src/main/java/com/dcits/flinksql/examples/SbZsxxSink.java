package com.dcits.flinksql.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import utils.ConnectionPool;
import utils.MySqlUtil;
import utils.PropertyUtil;

public class SbZsxxSink extends RichSinkFunction<Tuple2<Boolean, Row>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
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
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }
 
    @Override
    public void invoke(Tuple2<Boolean,Row> value, Context context) throws Exception {
        //如果是false，则删除。如果为true，则新增。
        Object[] o = new Object[0];
//        DbHelperImpl dbHelper;
//        if(value.f0==true){
//            MysqlUtil.executeSQL("",o, connection);
//        } else{
//
//        }
    }
}
