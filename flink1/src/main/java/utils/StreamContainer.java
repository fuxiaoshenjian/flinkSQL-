package utils;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.HashMap;
import java.util.Map;

/**
 * 保存所有流表、datastream对象的状态。
 */
public class StreamContainer {
    public static Map<String, DataStream> streamMap = new HashMap();
}
