package utils;

import com.dcits.flink.sql.DoubleJoin;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.util.Map;

public class TypeUtil {

    public static void getStringRow(Row row, String type, String value, int pos){
        switch (type) {
            case "int":row.setField(0, Integer.parseInt(value));
            case "string":row.setField(0, value);
            case "float":row.setField(0, Float.parseFloat(value));
            case "double":row.setField(0, Double.parseDouble(value));
        }
    }

    public static TypeInformation[] getTypeInfo(String table){
        Map<String, String> m = (Map) MetadataLoader.table.get(table);
        TypeInformation[] fieldTypes = new  TypeInformation[m.size()];
        int i = 0;
        for(Map.Entry<String, String> entry:m.entrySet()){
            switch (entry.getValue()) {
                case "int":fieldTypes[i] = BasicTypeInfo.INT_TYPE_INFO;
                case "string":fieldTypes[i] = BasicTypeInfo.STRING_TYPE_INFO;
                case "float":fieldTypes[i] = BasicTypeInfo.FLOAT_TYPE_INFO;
                case "double":fieldTypes[i] = BasicTypeInfo.DOUBLE_TYPE_INFO;
                case "INT":fieldTypes[i] = BasicTypeInfo.INT_TYPE_INFO;
                case "STRING":fieldTypes[i] = BasicTypeInfo.STRING_TYPE_INFO;
                case "FLOAT":fieldTypes[i] = BasicTypeInfo.FLOAT_TYPE_INFO;
                case "DOUBLE":fieldTypes[i] = BasicTypeInfo.DOUBLE_TYPE_INFO;
            }
        }
        return fieldTypes;
    }

    /**
     * 获取一个表所有列的类型，逗号分隔
     * @param table
     * @return
     */
    public static String allTypes(String table){
        Map<String, String> m = (Map) MetadataLoader.table.get(table);
        StringBuffer types = new StringBuffer();
        int i=0;
        for(String key:m.keySet()){
            types.append(key);
            i++;
            if(i<m.size()){
                types.append(",");
            }
        }
        return types.toString();
    }
}
