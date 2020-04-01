package de.kp.works.connect.ignite;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.cache.Cache;

import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.connect.BaseConfig;

public class IgniteConfig extends BaseConfig {

	private static final long serialVersionUID = 2258065855745759770L;

	@Description("The host of the Apache Ignite cluster.")
	@Macro
	public String host;

	@Description("The port of the Apache Ignite cluster.")
	@Macro
	public Integer port;

	@Description("The name of the Apache Ignite cache used to organize data.")
	@Macro
	public String cacheName;

	@Description("The comma-separated list of field names that are used to extract from the specified Ignite cache.")
	@Macro
	public String fieldNames;

	@Description("The number of partitions to organize the data of the specified Ignite cache. Default is 1.")
	@Macro
	public int partitions;
	
	public IgniteConfig() {
		partitions = 1;
	}
	
	public Properties getConfig() {
		// TODO
		return null;
	}
		
	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(host)) {
			throw new IllegalArgumentException(
					String.format("[%s] The cluster host must not be empty.", this.getClass().getName()));
		}

		if (port < 1) {
			throw new IllegalArgumentException(
					String.format("[%s] The cluster port must be positive.", this.getClass().getName()));
		}

		if (Strings.isNullOrEmpty(cacheName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The cache name must not be empty.", this.getClass().getName()));
		}

		if (Strings.isNullOrEmpty(fieldNames)) {
			throw new IllegalArgumentException(
					String.format("[%s] The cache field names must not be empty.", this.getClass().getName()));
		}

		if (partitions < 1) {
			throw new IllegalArgumentException(
					String.format("[%s] The number of partitions must be positive.", this.getClass().getName()));
		}
		
	}

	private Map<String, Integer> getIndices() {

		Map<String, Integer> fieldIndices = new HashMap<>();
		
		String[] tokens = fieldNames.split(",");
		
		for (int i= 0; i < tokens.length; i++) {
			String fieldName = tokens[i].trim();
			fieldIndices.put(fieldName, i);
		}
		
		return fieldIndices;
		
	}
	
	private String[] getFieldNames() {
		
		String[] tokens = fieldNames.split(",");
		String[] names = new String[tokens.length];
		
		for (int i= 0; i < tokens.length; i++) {
			names[i] = tokens[i].trim();
		}
		
		return names;
		
	}
	
	public Schema getSchema() {

		Schema schema = null;
		List<Schema.Field> fields = new ArrayList<>();
		
		try {
			/*
			 * We expect here that the Apache Ignite 
			 * context is initiated properly already
			 */
			IgniteClient ignite = IgniteContext.getInstance().getClient();
			ClientCache<String, org.apache.ignite.binary.BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();
			/*
			 * STEP #2: Build ScanQuery
			 */
			ScanQuery<String, org.apache.ignite.binary.BinaryObject> scan = new ScanQuery<>();
			scan.setPageSize(10);

			List<Cache.Entry<String,org.apache.ignite.binary.BinaryObject>> entries = cache.query(scan).getAll();
			if (entries.size() == 0)
				throw new IllegalArgumentException(String.format("The cache '%s' provided has no entries", cacheName));
						
			org.apache.ignite.binary.BinaryObject binaryObject = entries.get(0).getValue();
			for (String fieldName: getFieldNames()) {
				Object fieldValue = binaryObject.field(fieldName);
				fields.add(Schema.Field.of(fieldName, value2Schema(fieldValue)));
			}
			
			schema = Schema.recordOf("ignite", fields);

		} catch (Exception e) {
			schema = Schema.recordOf("ignite", fields);

		}

		return schema;

	}
	
	public StructuredRecord values2Record(List<Object> values, Schema schema) {

		Map<String, Integer> indices = getIndices();
		
		StructuredRecord.Builder builder = StructuredRecord.builder(schema);
		for (Schema.Field field : schema.getFields()) {

			String fieldName = field.getName();
			Integer fieldIndex = indices.get(fieldName);
			/*
			 * The list of values contains the _key field
			 * as its initial or first value; we therefore
			 * have to increment by one to get user field
			 * values
			 */			
			Object fieldValue = values.get(fieldIndex + 1);
			/*
			 * We do not expect nullable field schemas;
			 * therefore no addition check is performed
			 */
			Schema fieldSchema = field.getSchema();
			/*
			 * Apache Ignite supports date & time data
			 * types which are all mapped onto LONG 
			 */
			if (fieldSchema.getType().equals(Schema.Type.LONG))
				fieldValue = datetime2Long(fieldValue);
			
			builder.set(fieldName, fromFieldValue(fieldValue, fieldSchema));

		}

		return builder.build();

	}

	private Object datetime2Long(Object value) {
		
		/*
		 * Java Date, Time & Timestamp are mapped
		 * into a LONG type 
		 */
		
		if (value instanceof java.sql.Date)
			return ((java.sql.Date)value).getTime();
		
		else if (value instanceof java.sql.Time)
			return ((java.sql.Time)value).getTime();
		
		else if (value instanceof java.sql.Timestamp)
			return ((java.sql.Timestamp)value).getTime();
		
		else
			return value;
		
	}
	
	private Object fromFieldValue(Object value, Schema schema) {

		switch (schema.getType()) {
		/** BASIC DATA TYPES **/
		case DOUBLE:
		case FLOAT:
		case INT:
		case STRING:
			return value;
		/*
		 * Java Date, Time & Timestamp are mapped
		 * into a LONG type 
		 */
		case LONG:
			return datetime2Long(value);

		/** COMPLEX DATA TYPES **/
		case BYTES:
			return ByteBuffer.wrap((byte[]) value);

		case ARRAY: {

			/* value must be collection */
			Collection<?> collection = (Collection<?>) value;
			List<Object> result = new ArrayList<>(collection.size());

			for (Object element : collection) {
				/*
				 * Nullable is not relevant here: see schema inference
				 */
				Schema componentSchema = schema.getComponentSchema();
				result.add(fromFieldValue(element, componentSchema));

			}

			return result;
		}
		case MAP: {

			/* value mus be a map */
			Map<?, ?> map = (Map<?, ?>) value;
			Map<Object, Object> result = new LinkedHashMap<>(map.size());

			Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();

			Schema keySchema = mapSchema.getKey();
			Schema valueSchema = mapSchema.getValue();

			for (Map.Entry<?, ?> entry : map.entrySet()) {
				result.put(fromFieldValue(entry.getKey(), keySchema), fromFieldValue(entry.getValue(), valueSchema));
			}

			return result;

		}
		default:
			throw new IllegalArgumentException(
					String.format("Data type '%s' is not supported.", schema.getType().name()));
		}

	}

	/*
	 * A helper method to infer the data type from the
	 * field value of an Ignite BinaryObject 
	 */
	public Schema value2Schema(Object value) {

		Schema schema = null;
		/** BASIC TYPES **/
		if (value instanceof Boolean) 
			schema = Schema.of(Schema.Type.BOOLEAN);

		else if (value instanceof Double)
			schema = Schema.of(Schema.Type.DOUBLE);

		else if (value instanceof Float)
			schema = Schema.of(Schema.Type.FLOAT);

		else if (value instanceof Integer)
			schema = Schema.of(Schema.Type.INT);

		else if (value instanceof Long)
			schema = Schema.of(Schema.Type.LONG);
		/*
		 * A [Short] type should not occur, referring to Ignite's data type
		 * documentation
		 */
		else if (value instanceof Short)
			schema = Schema.of(Schema.Type.LONG);

		else if (value instanceof String)
			schema = Schema.of(Schema.Type.STRING);

		/** DATE & TIME TYPES **/
		
		/*
		 * Java Date, Time & Timestamp are mapped
		 * into a LONG type 
		 */
		
		else if (value instanceof java.sql.Date)
			schema = Schema.of(Schema.Type.LONG);
		
		else if (value instanceof java.sql.Time)
			schema = Schema.of(Schema.Type.LONG);
		
		else if (value instanceof java.sql.Timestamp)
			schema = Schema.of(Schema.Type.LONG);
		
		
		/** COMPLEX DATA TYPES **/
		else if (value instanceof Collection<?>) {

			List<Object> values = new ArrayList<>((Collection<?>) value);
			Object head = values.get(0);
			/*
			 * Special treatment of Byte Arrays
			 */
			if (head instanceof Byte) {
				schema = Schema.of(Schema.Type.BYTES);

			} else {
				schema = Schema.arrayOf(value2Schema(head));

			}

		} else if (value instanceof Map<?, ?>) {
			/*
			 * The support of MAP is left here, even if we have
			 * no evidence that Ignite caches are able to cope
			 * with maps
			 */
			Map<Object, Object> map = new HashMap<>((Map<?, ?>) value);
			Map.Entry<Object, Object> head = map.entrySet().iterator().next();

			schema = Schema.mapOf(value2Schema(head.getKey()), value2Schema(head.getValue()));

		} else if (value.getClass().isArray()) {

			List<Object> values = new ArrayList<>(Arrays.asList((Object[]) value));
			Object head = values.get(0);
			/*
			 * Special treatment of Byte Arrays
			 */
			if (head instanceof Byte) {
				schema = Schema.of(Schema.Type.BYTES);

			} else {
				schema = Schema.arrayOf(value2Schema(head));

			}

		}

		return schema;

	}

}
