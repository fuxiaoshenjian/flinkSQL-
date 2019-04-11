package com.dcits.flinksql.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public final void flatMap(String value,
				Collector<Tuple2<String, Integer>> out) throws Exception {
			// TODO Auto-generated method stub
	        String[] tokens = value.toLowerCase().split("\\W+");

	        // emit the pairs
	        for (String token : tokens) {
	            if (token.length() > 0) {
	                out.collect(new Tuple2<String, Integer>(token, 1));
	            }   
	        }
		}
	}