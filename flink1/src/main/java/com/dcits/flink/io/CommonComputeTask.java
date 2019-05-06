package com.dcits.flink.io;

import com.dcits.flink.model.DjNsrxx;
import org.apache.flink.types.Row;
import utils.MetadataLoader;
import utils.MySqlUtil;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class CommonComputeTask implements Callable<List<Row>> {

    private Row row;
    private String table;

    public CommonComputeTask(Row row, String currentTable){
        this.row = row;
        this.table = currentTable;
    }

    @Override  
    public List<Row> call() throws Exception {
        // TODO Auto-generated method stub
        int amount = 0;
        String joinTable = (String) ((Map) MetadataLoader.map.get(table)).get("table");
        Object[] param = getParam(row);
        List<String> jColName = (List) ((Map) MetadataLoader.map.get(table)).get("col");

    	StringBuffer sb = new StringBuffer();
    	sb.append("select * from ").append(joinTable).append(" t where ");
    	for(String x:jColName){
            sb.append(x).append("= ").append("?");
        }
    	MySqlUtil mysql = new MySqlUtil();
    	ResultSet rs = mysql.executeQuery(sb.toString());

    	List<DjNsrxx> n = new ArrayList<>();
        while(rs.next()){
            DjNsrxx dj = new DjNsrxx(rs.getString("nsrsbh"), rs.getString("djxh"),
                   rs.getString("IDF_ETLSJJZRQ"), rs.getString("IDF_FLAG"),
                   rs.getString("IDF_XH"));
            n.add(dj);

        }
        rs.close();
        return null;
    }


    public Object[] getParam(Row row){
        Map<String, Object> map = (Map) MetadataLoader.map.get(table);
        List<String> joinColoumns = (List) map.get("col");
        int size = joinColoumns.size();
        Map<String, Map> tablemeta = (Map) MetadataLoader.table.get(table);
        Object[] param = new Object[size];
        int n = 0;
        for(String key:  tablemeta.keySet()){
            /**找col中对象在row中的相对位置*/
            if(joinColoumns.contains(key)){
                param[n] = row.getField(n);
            }
            n++;
        }return  param;
    }
}