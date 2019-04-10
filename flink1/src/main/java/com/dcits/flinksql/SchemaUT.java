package com.dcits.flinksql;

import java.io.IOException;

import org.apache.calcite.adapter.java.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;

public class SchemaUT implements DeserializationSchema, SerializationSchema{
	public Long user;
	public String product;
	public int amount;

	public SchemaUT() {
	}

	public SchemaUT(Long user, String product, int amount) {
		this.user = user;
		this.product = product;
		this.amount = amount;
	}

	@Override
	public String toString() {
		return "Order{" +
			"user=" + user +
			", product='" + product + '\'' +
			", amount=" + amount +
			'}';
	}

	@Override
	public TypeInformation getProducedType() {
		// TODO Auto-generated method stub
		return BasicTypeInfo.of(com.dcits.flinksql.StreamSQLExample.Order.class);
	}

	@Override
	public byte[] serialize(Object arg0) {
		// TODO Auto-generated method stub
		String x = 	"Order{" +
				"user=" + user +
				", product='" + product + '\'' +
				", amount=" + amount +
				'}';
		
		return x.getBytes();
	}

	@Override
	public Object deserialize(byte[] abyte0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isEndOfStream(Object arg0) {
		// TODO Auto-generated method stub
		return false;
	}
	
}
