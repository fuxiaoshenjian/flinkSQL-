package com.dcits.flink.model;

public class SbZsxx {
    public String se;
    public String zsxm_dm;
    public String zsxh;
    public String djxh;
    public String IDF_ETLSJJZRQ;
    public String IDF_FLAG;
    public String IDF_XH;

    public SbZsxx(){}

    public SbZsxx(String params){
        se = params.split(",")[0];
        zsxm_dm = params.split(",")[1];
        zsxh = params.split(",")[2];
        djxh = params.split(",")[3];
        IDF_ETLSJJZRQ = params.split(",")[4];
        IDF_FLAG = params.split(",")[5];
        IDF_XH = params.split(",")[6];
    }

    public SbZsxx(String se, String zsxm_dm, String zsxh, String djxh, String IDF_ETLSJJZRQ,
                  String IDF_FLAG, String IDF_XH){
        se = se;
        zsxm_dm = zsxm_dm;
        zsxh = zsxh;
        djxh = djxh;
        IDF_ETLSJJZRQ = IDF_ETLSJJZRQ;
        IDF_FLAG = IDF_FLAG;
        IDF_XH = IDF_XH;
    }

    public static String getFields(){
        return "se,zsxm_dm,zsxh,djxh,IDF_ETLSJJZRQ,IDF_FLAG,IDF_XH";
    }

    public String getSe() {
        return se;
    }

    public void setSe(String se) {
        this.se = se;
    }

    public String getZsxm_dm() {
        return zsxm_dm;
    }

    public void setZsxm_dm(String zsxm_dm) {
        this.zsxm_dm = zsxm_dm;
    }

    public String getZsxh() {
        return zsxh;
    }

    public void setZsxh(String zsxh) {
        this.zsxh = zsxh;
    }

    public String getDjxh() {
        return djxh;
    }

    public void setDjxh(String djxh) {
        this.djxh = djxh;
    }

    public String getIDF_ETLSJJZRQ() {
        return IDF_ETLSJJZRQ;
    }

    public void setIDF_ETLSJJZRQ(String IDF_ETLSJJZRQ) {
        this.IDF_ETLSJJZRQ = IDF_ETLSJJZRQ;
    }

    public String getIDF_FLAG() {
        return IDF_FLAG;
    }

    public void setIDF_FLAG(String IDF_FLAG) {
        this.IDF_FLAG = IDF_FLAG;
    }

    public String getIDF_XH() {
        return IDF_XH;
    }

    public void setIDF_XH(String IDF_XH) {
        this.IDF_XH = IDF_XH;
    }
}
