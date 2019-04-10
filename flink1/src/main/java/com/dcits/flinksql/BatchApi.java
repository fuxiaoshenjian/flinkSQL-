package com.dcits.flinksql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批量读取文件系统的数据
 *
 */
public class BatchApi 
{
    public static void main( String[] args )
    {
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    	DataSet<String> text = env.readTextFile("D:\\data\\hosts");
    	
    	DataSet<Tuple2<String, Integer>> counts =
    	        // split up the lines in pairs (2-tuples) containing: (word,1)
    	        text.flatMap(new Tokenizer())
    	        // group by the tuple field "0" and sum up tuple field "1"
    	        .groupBy(0)
    	        .sum(1);
    	try {
			System.out.println(counts.count());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	String outputPath = "D:\\data\\hosts\\flink.txt";
		counts.writeAsCsv(outputPath , "\n", " ");

    	// User-defined functions
    }
}
