package com.dcits.flink.model;

public class DjNsrxx {

    public String nsrsbh;
    public String djxh;
    public String IDF_ETLSJJZRQ;
    public String IDF_FLAG;
    public String IDF_XH;

    public DjNsrxx(){}

    public DjNsrxx(String params){
        nsrsbh = params.split(",")[0];
        djxh = params.split(",")[1];
        IDF_ETLSJJZRQ = params.split(",")[2];
        IDF_FLAG = params.split(",")[3];
        IDF_XH = params.split(",")[4];
    }

    public DjNsrxx(String nsrsbh, String djxh, String IDF_ETLSJJZRQ, String IDF_FLAG, String IDF_XH){
        nsrsbh = nsrsbh;
        djxh = djxh;
        IDF_ETLSJJZRQ =IDF_ETLSJJZRQ;
        IDF_FLAG = IDF_FLAG;
        IDF_XH = IDF_XH;
    }

    public static String getFields(){
        return "nsrsbh,djxh,IDF_ETLSJJZRQ,IDF_FLAG,IDF_XH";
    }
    @Override
    public String toString(){
        return "nsrsbh:"+nsrsbh+","+"djxh:"+djxh+","+"IDF_ETLSJJZRQ:"+
                IDF_ETLSJJZRQ+","+"IDF_FLAG:"+IDF_FLAG+","+"IDF_XH:"+IDF_XH;
    }
}
