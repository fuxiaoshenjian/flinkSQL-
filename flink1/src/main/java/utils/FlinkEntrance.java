package utils;

import com.dcits.flink.sql.DoubleJoin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FlinkEntrance {

    private final static Log log = LogFactory.getLog(FlinkEntrance.class);

    static StreamExecutionEnvironment env;
    static StreamTableEnvironment tableEnv;
    static StreamQueryConfig qConfig;
    static final int parallelism = 10;
    static Properties properties;
    static Map<String, Map> graph;
    static Map<String, DataStream> streamMap;
    static Queue<String> undo;
    static{
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnv = TableEnvironment.getTableEnvironment(env);
        env.setParallelism(parallelism);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamQueryConfig qConfig = tableEnv.queryConfig();
        undo = new ConcurrentLinkedQueue();
        qConfig.withIdleStateRetentionTime(Time.hours(12), Time.hours(16));
    }

    public static void main(String args[]){
        graph = MetadataLoader.graph;
        /**衍生节点*/
        Set<String> generatedNode = new HashSet<>();
        /**源节点*/
        Set<String> originNode = new HashSet<>();
        for(String k:graph.keySet()){
            Map m = graph.get(k);
            if(m.containsKey("nextNodes")){
                generatedNode.add(k);
            }
        }
        for(String k:graph.keySet()){
            if(!generatedNode.contains(k)){
                originNode.add(k);
            }
        }
        for(String s:originNode){
            undo.add(s);
        }
        genProcessGraph();

    }

    /**
     * 广度优先
     */
    public static void genProcessGraph(){
        while(true){
            String key = "";
            if((key = undo.poll())==null){break;};
            Map meta = graph.get(key);
            List<String> childs = handle(meta);
            /**若没有处理过，添加该节点*/
            for(String child : childs){
                if(!undo.contains(child)){
                    undo.add(child);
                }
            }
        }
    }

    /**
     * 单节点的处理逻辑。返回下一节点List！！！！！！
     * @param m : 节点元数据
     * @return
     */
    public static List<String> handle(Map m){
        //TODO: 提前注册下一节点需要用到的tables
        Map<String, Map> input = (Map) m.get("input");
        if(input.size()==0){
            log.error("input节点内容为空！");
        }
        if(m.get("op").equals("join")){
            DoubleJoin join = new DoubleJoin(env, tableEnv, m, streamMap);
            /**fuck not like this ！！
             *节点输出均为 kafka topic读入
             * */
            join.KjK();
            /**节点输入均为注册过的table*/
            join.TjT();
        }
        return new ArrayList<>();
    }
}
