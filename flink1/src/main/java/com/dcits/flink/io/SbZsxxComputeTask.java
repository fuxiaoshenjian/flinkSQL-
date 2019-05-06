package com.dcits.flink.io;

import com.dcits.flink.model.SbZsxx;
import utils.MySqlUtil;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class SbZsxxComputeTask implements Callable<List<SbZsxx>> {

    private String djxh = "";

    public SbZsxxComputeTask(String djxh){
        this.djxh = djxh;
    }

    @Override  
    public List<SbZsxx> call() throws Exception {
        // TODO Auto-generated method stub  
        int amount = 0;
    	MySqlUtil mysql = new MySqlUtil();
    	List<SbZsxx> sbs = new ArrayList<>();

    	ResultSet rs = mysql.executeQuery("select * from ods_sb_zsxx t where t.djxh='"+djxh+"'");
    	while(rs.next()){
            SbZsxx sb = new SbZsxx(rs.getString("se"), rs.getString("zsxm_dm"),
                    rs.getString("zsxh"), rs.getString("djxh"),
                    rs.getString("IDF_ETLSJJZRQ"), rs.getString("IDF_FLAG"),
                    rs.getString("IDF_XH"));
            sbs.add(sb);
        }
        rs.close();
        return sbs;
    }  
}  