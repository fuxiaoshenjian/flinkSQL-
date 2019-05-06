package com.dcits.flink.io;

import com.dcits.flink.model.DjNsrxx;
import utils.MySqlUtil;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class DjNsrComputeTask implements Callable<List<DjNsrxx>> {

    private String djxh;

    public DjNsrComputeTask(String djxh){
        this.djxh = djxh;
    }

    @Override  
    public List<DjNsrxx> call() throws Exception {
        // TODO Auto-generated method stub
        int amount = 0;
    	MySqlUtil mysql = new MySqlUtil();
    	ResultSet rs = mysql.executeQuery("select * from ods_dj_nsrxx t where t.djxh='"+djxh+"'");

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
}