package de.kp.works.connect.aerospike;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.google.gson.Gson;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

public class AerospikeRecordWritable implements Writable, AerospikeWritable, Configurable {

	private StructuredRecord record;
	private Configuration conf;

	private Gson GSON = new Gson();
	
	public AerospikeRecordWritable(StructuredRecord record) {
		this.record = record;
	}

	/**
	 * Used in map-reduce. Do not remove.
	 */
	public AerospikeRecordWritable() {
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		throw new IOException(String.format("[%s] Method 'readFields' from DataInput is not implemented", this.getClass().getName()));
	}

	@Override
	public void write(DataOutput output) throws IOException {
		throw new IOException(String.format("[%s] Method 'write' from DataOutput is not implemented", this.getClass().getName()));
	}

	@Override
	public void write(AerospikeClient client, WritePolicy policy, String namespace, String setName) {

		Map<String,Object> fieldMap = new HashMap<>();
		
		List<Bin> bins = new ArrayList<>();
		for (Schema.Field field: record.getSchema().getFields()) {
				
			String fieldName = field.getName();
			Object fieldValue = record.get(fieldName);

			Schema.Type fieldType = getType(field);
			fieldMap.put(fieldName, fieldValue);
			
			bins.add(field2Bin(fieldName, fieldValue, fieldType));

		}
		/*
		 * The key is built from the namespace, the set name 
		 * and the MD5 value from the serialized field map;
		 * 
		 * this avoids to provide plenty of configurations
		 * to specify the content of an Aerospike record
		 */
		String serialized = GSON.toJson(fieldMap);
		String uid = null;
		try {
			uid = MessageDigest.getInstance("MD5").digest(serialized.getBytes()).toString();

		} catch (Exception e) {
			uid = serialized;
		}
		
		Bin[] values = new Bin[bins.size()];
        values = bins.toArray(values);		
		
		Key key = new Key(namespace, setName, uid);
        client.put(policy, key, values);
		
	}
	
	/*
	 * A helper method to retrieve the field type
	 */
	private Schema.Type getType(Schema.Field field) {
		
		Schema schema = field.getSchema();
		if (schema.isNullable())
			return schema.getNonNullable().getType();
		
		else
			return schema.getType();
		
	}
	  
	private Bin field2Bin(String fieldName, Object fieldValue, Schema.Type fieldType) {
				
		/*
		 * We do not check whether the respective field type
		 * is nullable or not here
		 */
		if (fieldValue == null) return Bin.asNull(fieldName);
		
		Bin bin = null;
		switch(fieldType) {
		/** BASIC DATA TYPE **/
		case BOOLEAN: {
			bin = new Bin(fieldName, (Boolean)fieldValue);
			break;			
		}
		case BYTES: {
			bin = new Bin(fieldName, (byte[])fieldValue);
			break;
		}
		case DOUBLE: {
			bin = new Bin(fieldName, (Double)fieldValue);
			break;			
		}
		case FLOAT: {
			bin = new Bin(fieldName, (Float)fieldValue);
			break;			
		}
		case INT: {
			bin = new Bin(fieldName, (Integer)fieldValue);
			break;			
		}
		case LONG: {
			bin = new Bin(fieldName, (Long)fieldValue);
			break;			
		}
		case STRING: {
			bin = new Bin(fieldName, (String)fieldValue);
			break;
		}
		/** COMPLEX DATA TYPES **/
		case ARRAY: {

			Collection<?> collection = (Collection<?>) fieldValue;
	        List<Object> result = new ArrayList<>(collection);
			
	        bin = new Bin(fieldName, result);
			break;
			
		}
		case MAP: {
	        Map<?, ?> map = (Map<?, ?>) fieldValue;
	        Map<Object, Object> result = new HashMap<>(map);
			
	        bin = new Bin(fieldName, result);
			break;
			
		}
		default: {
			bin = Bin.asNull(fieldName);
		}
		}
		
		return bin;
	}

}
