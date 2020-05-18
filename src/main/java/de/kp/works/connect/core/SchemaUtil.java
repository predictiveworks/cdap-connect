package de.kp.works.connect.core;
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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.Random;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import co.cask.cdap.api.data.schema.Schema;

public class SchemaUtil implements Serializable {

	private static final long serialVersionUID = 3399224989175030778L;
	
	/**
	 * Infer a schema from a number of samples; number of samples must
	 * be large enough to even cover missing or optional fields as well
	 * 
	 * @param jsonArray
	 * @param samples
	 * @return
	 */
	public static Schema inferSchema(JsonArray jsonArray, Integer samples) {
		
		List<JsonObject> list = new ArrayList<>();
		jsonArray.forEach(item -> list.add(item.getAsJsonObject()));

		return inferSchema(list, samples);
		
	}
	/**
	 * Infer a schema from a number of samples
	 * 
	 * @param jsonArray
	 * @param samples
	 * @return
	 */
	public static Schema inferSchema(List<JsonObject> jsonArray, Integer samples) {
		/*
		 * STEP #1: Retrieve samples of schemas from randomly selected [JsonObject]
		 */
		List<Schema> schemas = new ArrayList<>();
		List<String> fieldNames = new ArrayList<>();

		int size = jsonArray.size();
		Random random = new Random();
		/*
		 * Randomly select samples of schema from the provided [JsonArray]
		 */
		for (int i = 0; i < samples; i++) {

			int index = random.nextInt(size);
			/*
			 * Retrieve schema and also the unique list of all field names to determine
			 * whether a certain schema must be nullable or not
			 */
			Schema schema = inferSchema(jsonArray.get(index).getAsJsonObject());
			schema.getFields().stream().forEach(field -> {

				String fieldName = field.getName();

				if (!fieldNames.contains(fieldName))
					fieldNames.add(fieldName);

			});

			schemas.add(schema);

		}
		/*
		 * STEP #2: We compare the respective fields
		 */
		if (fieldNames.isEmpty())
			return null;

		List<Schema.Field> fields = new ArrayList<>();

		for (String fieldName : fieldNames) {
			fields.add(inferField(schemas, fieldName));
		}

		Schema schema = Schema.recordOf("jsonSchema", fields);
		return schema;
		
	}
	
	public static Schema.Field inferField(List<Schema> schemas, String fieldName) {

		Schema fieldSchema = null;
		boolean isNullable = false;

		for (Schema schema : schemas) {

			Schema.Field schemaField = schema.getField(fieldName);
			/*
			 * If there is a sample schema that does not contain the respective field, then
			 * the final field schema is set to NULLABLE
			 */
			if (schemaField == null) {
				isNullable = true;

			} else {
				/*
				 * If the current field is the first field of the list, then no field comparison
				 * is carried out
				 */
				if (fieldSchema == null)
					fieldSchema = schemaField.getSchema();

				else {
					/*
					 * In this case, we are able to compare both fields and merge the respective
					 * result
					 */
					Schema mergedSchema = mergeSchemas(fieldSchema, schemaField.getSchema());
					if (mergedSchema != null)
						fieldSchema = mergedSchema;

				}

			}

		}

		if (isNullable == true && fieldSchema.isNullable() == false)
			fieldSchema = Schema.nullableOf(fieldSchema);

		return Schema.Field.of(fieldName, fieldSchema);

	}

	private static Schema mergeSchemas(Schema schema, Schema other) {

		/* Compare schemas */

		if (schema.equals(other))
			return schema;

		/*
		 * Check whether the non nullable part is equal
		 */
		Schema schemaNonNull = schema.isNullable() ? schema.getNonNullable() : schema;
		Schema otherNonNull = other.isNullable() ? other.getNonNullable() : other;
		/*
		 * If one of the schemas is NULLABLE then the merged schema is also NULLABLE
		 */
		if (schemaNonNull.equals(otherNonNull))
			return Schema.nullableOf(schemaNonNull);

		Schema.Type schemaType = schemaNonNull.getType();
		Schema.Type otherType = otherNonNull.getType();
		/*
		 * We expect that the schema types are different, and in this case we claim that
		 * at least of the types is NULL
		 */
		if (schemaType.equals(Schema.Type.NULL)) {
			return Schema.nullableOf(otherNonNull);

		} else if (otherType.equals(Schema.Type.NULL)) {
			return Schema.nullableOf(schemaNonNull);

		} else
			return null;

	}

	public static Schema inferSchema(JsonObject jsonObject) {

		List<Schema.Field> fields = new ArrayList<>();

		for (Entry<String, JsonElement> entry : jsonObject.entrySet()) {

			String entryName = entry.getKey();
			JsonElement entryValue = entry.getValue();

			Schema.Field field = Schema.Field.of(entryName, feature2Schema(entryValue));
			fields.add(field);

		}

		Schema schema = Schema.recordOf("jsonSchema", fields);
		return schema;

	}

	private static Schema feature2Schema(JsonElement feature) {

		if (feature.isJsonNull())
			return Schema.of(Schema.Type.NULL);

		else if (feature.isJsonPrimitive()) {
			JsonPrimitive value = feature.getAsJsonPrimitive();
			return primitive2Schema(value);

		}

		/** COMPLEX DATA TYPES **/
		else if (feature.isJsonArray()) {
			/*
			 * We check whether the JSON array is well-formatted, i.e. each item refers to
			 * the same JSON format
			 */
			JsonArray items = feature.getAsJsonArray();

			Integer index = getIndex(items);
			if (index > -1) {

				JsonElement item = items.get(index);
				return Schema.arrayOf(feature2Schema(item));

			} else
				return Schema.arrayOf(Schema.of(Schema.Type.NULL));

		} else if (feature.isJsonObject()) {

			List<Schema.Field> fields = new ArrayList<>();

			JsonObject item = feature.getAsJsonObject();
			for (Entry<String, JsonElement> entry : item.entrySet()) {

				String entryName = entry.getKey();
				JsonObject entryValue = entry.getValue().getAsJsonObject();
				/*
				 * Value is an object with two fields, 'type' and 'value'
				 */
				Schema.Field field = Schema.Field.of(entryName, feature2Schema(entryValue));
				fields.add(field);

			}

			return Schema.recordOf(UUID.randomUUID().toString(), fields);

		}

		return null;

	}

	public static Schema primitive2Schema(JsonPrimitive value) {
		
		if (value.isBoolean())
			return Schema.of(Schema.Type.BOOLEAN);

		else if (value.isNumber()) {
			/*
			 * Gson does not support the derivation of the basic type of [Number]; we
			 * therefore wrap it as an [Object] with a limited amount of baic data types
			 */
			Object number = wrapNumber(value.getAsNumber());

			if (number instanceof BigDecimal)
				return Schema.of(Schema.Type.DOUBLE);

			else if (number instanceof BigInteger)
				return Schema.of(Schema.Type.LONG);

			else if (number instanceof Double)
				return Schema.of(Schema.Type.DOUBLE);

			else if (number instanceof Integer)
				return Schema.of(Schema.Type.INT);

			else if (number instanceof Long)
				return Schema.of(Schema.Type.LONG);
			else
				throw new IllegalArgumentException(
						"Supported basic data types: Double, Float, Integer, Long and Short.");

		} else if (value.isString())
			return Schema.of(Schema.Type.STRING);
		
		else
			return null;
	
	}
	
	private static Object wrapNumber(Number n) {

		Object number;

		BigDecimal bigDecimal = new BigDecimal(n.toString());
		if (bigDecimal.scale() <= 0) {

			if (bigDecimal.compareTo(new BigDecimal(Integer.MAX_VALUE)) <= 0) {
				number = bigDecimal.intValue();

			} else if (bigDecimal.compareTo(new BigDecimal(Long.MAX_VALUE)) <= 0) {
				number = bigDecimal.longValue();

			} else {
				number = bigDecimal;
			}

		} else {

			final double doubleValue = bigDecimal.doubleValue();
			if (BigDecimal.valueOf(doubleValue).compareTo(bigDecimal) != 0) {
				number = bigDecimal;
			} else {
				number = doubleValue;
			}
		}

		return number;
	}

	private static Integer getIndex(JsonArray jsonArray) {

		Integer index = -1;

		Integer count = Math.min(10, jsonArray.size());
		for (int i = 0; i < count; i++) {
			/*
			 * We look for the first element that is different from NULL and does not
			 * describe an ARRAY
			 */
			JsonElement item = jsonArray.get(i);
			if (item.isJsonNull() || item.isJsonArray())
				continue;

			index = i;
			break;

		}

		return index;
	}

}
