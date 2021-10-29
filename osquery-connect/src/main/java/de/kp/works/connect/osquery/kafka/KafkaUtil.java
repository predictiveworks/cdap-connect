package de.kp.works.connect.osquery.kafka;

/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Set;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

public class KafkaUtil {

	public static Object valueToObject(Schema schema, JsonElement value) {
		
		switch (getNonNullIfNullable(schema).getType()) {
		case NULL:
			return null;
		case BOOLEAN:
			return value.getAsBoolean();
		case BYTES: {
			/* We expect that the JsonElement is an Array of Bytes */
			if (!value.isJsonArray())
				throw new IllegalArgumentException("JSON Array expected but JSON Object found.");

			JsonArray jsonBytes = value.getAsJsonArray();

			byte[] bytes = new byte[jsonBytes.size()];
			for (int i = 0; i < jsonBytes.size(); i++) {
				bytes[i] = jsonBytes.get(i).getAsByte();
			}

			return ByteBuffer.wrap(bytes);
		}
		case DOUBLE:
			return value.getAsDouble();
		case FLOAT:
			return value.getAsFloat();
		case INT:
			return value.getAsInt();
		case LONG:
			return value.getAsLong();
		case STRING:
			return value.getAsString();
		case ARRAY: {
			/* We expect that the JsonElement is an Array */
			if (!value.isJsonArray())
				throw new IllegalArgumentException("JSON Array expected but other JSON element found.");

			JsonArray jsonArray = value.getAsJsonArray();
			List<Object> result = new ArrayList<>();
			/*
			 * The component schema specifies the schema of the array component or null if
			 * this is not an ARRAY schema
			 */
			assert schema.getComponentSchema() != null;
			Schema valueSchema = getNonNullIfNullable(schema.getComponentSchema());

			for (int i = 0; i < jsonArray.size(); i++) {

				JsonElement elem = jsonArray.get(i);
				if (elem.isJsonNull() && !schema.getComponentSchema().isNullable())
					throw new IllegalArgumentException("Null value is not allowed for array element.");

				result.add(valueToObject(valueSchema, elem));

			}
			return result;
		}
		case MAP: {
			/* We expect that the JsonElement is an Object */
			if (!value.isJsonObject())
				throw new IllegalArgumentException("JSON Object expected but other JSON element found.");

			Set<Entry<String, JsonElement>> entries = value.getAsJsonObject().entrySet();
			Map<String, Object> result = new LinkedHashMap<>(entries.size());
			/*
			 * In contrast to the more general MAP use case, a JsonObject always contains a
			 * String as key, i.e. the keySchema is not needed
			 */
			Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
			assert mapSchema != null;
			Schema valueSchema = getNonNullIfNullable(mapSchema.getValue());

			for (Entry<String, JsonElement> entry : entries) {
				if (entry.getValue() == null && !mapSchema.getValue().isNullable()) {
					throw new IllegalArgumentException("Null value is not allowed for MAP element.");
				}

				result.put(entry.getKey(), valueToObject(valueSchema, entry.getValue()));

			}

			return result;
		}
		case RECORD: {

			/* We expect that the JsonElement is an Object */
			if (!value.isJsonObject())
				throw new IllegalArgumentException("JSON Object expected but other JSON element found.");

			JsonObject jsonObject = value.getAsJsonObject();

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			assert schema.getFields() != null;
			for (Schema.Field field : schema.getFields()) {

				String fieldName = field.getName();
				Schema fieldSchema = getNonNullIfNullable(field.getSchema());

				JsonElement jsonElement = jsonObject.get(fieldName);
				if (jsonElement.isJsonNull() && !field.getSchema().isNullable()) {
					throw new IllegalArgumentException("Null value is not allowed for RECORD field value.");
				}
				builder.set(field.getName(), valueToObject(fieldSchema, jsonElement));

			}
			return builder.build();
		}
		case ENUM:
		case UNION:
			throw new IllegalArgumentException("Data types ENUM and UNION are not supported yet.");
		}

		throw new IllegalArgumentException("Data type " + schema.getType() + " not supported yet.");

	}

	/**
	 * This method extracts the content of a Kafka message and builds a structured
	 * record based on a provided schema specification
	 */
	public static void messageToRecord(byte[] message, StructuredRecord.Builder builder, Schema schema, Set<String> excludeFields) {

		String text = new String(message);
		
		Gson gson = new Gson();
		JsonObject json = gson.fromJson(text, JsonObject.class);
		/*
		 * The schema describes a record with an internally specified record name, such
		 * as 'Record1'; we have to extract the respective fields to build
		 */
		if (!schema.getType().equals(Schema.Type.RECORD))
			throw new IllegalArgumentException("Message schema must be specified as RECORD.");

		List<Schema.Field> fields = schema.getFields();
		assert fields != null;
		for (Schema.Field field : fields) {

			String fieldName = field.getName();
			/*
			 * Note, the internal _timestamp field value is set already and must not
			 * be processed here
			 */
			if (excludeFields.contains(fieldName)) continue;

			/*
			 * IMPORTANT: Nullable fields are described by a CDAP field schema
			 * that is a UNION of a the nullable type and the non-nullable type;
			 * 
			 * to evaluate the respective schema, we MUST RESTRICT to the non-
			 * nullable type only; otherwise we also view UNION schemas
			 */
			Schema fieldSchema = field.getSchema();
			Schema valueSchema = getNonNullIfNullable(fieldSchema);
			
			Object fieldValue = valueToObject(valueSchema, json.get(fieldName));

			builder.set(fieldName, fieldValue);

		}

	}

	  /**
	   * Returns the non-nullable part of the given {@link Schema} if it is nullable; otherwise return it as is.
	   */
	  private static Schema getNonNullIfNullable(Schema schema) {
	    return schema.isNullable() ? schema.getNonNullable() : schema;
	  }

}
