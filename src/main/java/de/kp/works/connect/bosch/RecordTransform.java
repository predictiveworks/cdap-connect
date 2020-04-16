package de.kp.works.connect.bosch;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.spark.api.java.function.Function;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

public class RecordTransform implements Function<String, StructuredRecord> {

	private static final long serialVersionUID = -8483973735620860323L;

	protected static final String MULTI_THING_SCHEMA = "multi.thing.schema";

	public RecordTransform() {
	}

	@Override
	public StructuredRecord call(String in) throws Exception {
		throw new Exception("not implemented");
	}

	/***** BUILD RECORD *****/
	
	protected void features2Record(StructuredRecord.Builder builder, Schema schema, JsonArray features) {

		Iterator<JsonElement> iter = features.iterator();
		while (iter.hasNext()) {
			/*
			 * A feature is described by one or more properties
			 */
			JsonObject feature = iter.next().getAsJsonObject();
			feature2Record(builder, schema, feature);
		}

	}

	protected void feature2Record(StructuredRecord.Builder builder, Schema schema, JsonObject feature) {
		/*
		 * A feature consists and an identifier and a list of properties
		 */
		String id = feature.get("id").getAsString();

		JsonArray properties = feature.get("properties").getAsJsonArray();
		Iterator<JsonElement> iter = properties.iterator();

		while (iter.hasNext()) {

			JsonObject property = iter.next().getAsJsonObject();
			String name = property.get("name").getAsString();

			String fieldName = id + "_" + name;
			Object fieldValue = property2Value(schema.getField(fieldName).getSchema(), property.get("value"));

			builder.set(fieldName, fieldValue);

		}

	}

	protected Object property2Value(Schema fieldSchema, JsonElement value) {

		Schema.Type fieldType = fieldSchema.getType();
		switch (fieldType) {
		case NULL: {
			return null;
		}
		case BOOLEAN: 
			return value.getAsBoolean();
		case DOUBLE: 
			return value.getAsDouble();
		case INT: 
			return value.getAsInt();
		case LONG: 
			return value.getAsLong();
		case STRING: 
			return value.getAsString();
		case ARRAY: {

			/* Input items */
			JsonArray items = value.getAsJsonArray();
			Iterator<JsonElement> iter = items.iterator();

			/* Output items */
			Schema componentSchema = fieldSchema.getComponentSchema();
			List<Object> output = new ArrayList<>(items.size());
			
			while (iter.hasNext()) {
				output.add(property2Value(componentSchema, iter.next()));
			}
			
			return output;
			
		}
		case RECORD: {
			
			/* Input items */
			JsonObject items = value.getAsJsonObject();
			
			/* Output items */
			StructuredRecord.Builder builder = StructuredRecord.builder(fieldSchema);			
			for (Entry<String, JsonElement> entry : items.entrySet()) {

				String entryName = entry.getKey();
				Object entryValue = property2Value(fieldSchema.getField(entryName).getSchema(), entry.getValue().getAsJsonObject().get("value"));

				builder.set(entryName, entryValue);
				
			}
			
			return builder.build();
			
		}
		default:
		}
		
		return null;
	}

	/***** BUILD SCHEMA *****/
	
	protected Schema buildSchema(String schemaType) {

		List<Schema.Field> schemaFields = new ArrayList<>();

		if (schemaType.equals(MULTI_THING_SCHEMA)) {

			/*
			 * Common schema fields
			 */
			Schema.Field timestamp = Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG));
			schemaFields.add(timestamp);

			Schema.Field name = Schema.Field.of("name", Schema.of(Schema.Type.STRING));
			schemaFields.add(name);

			Schema.Field namespace = Schema.Field.of("namespace", Schema.of(Schema.Type.STRING));
			schemaFields.add(namespace);
			/*
			 * Features can be different for different things; therefore a generic JSON
			 * representation is used
			 */
			Schema.Field features = Schema.Field.of("features", Schema.of(Schema.Type.STRING));
			schemaFields.add(features);

			Schema schema = Schema.recordOf("thingMessage", schemaFields);
			return schema;

		}

		return null;
	}

	protected List<Schema.Field> feature2Fields(JsonObject feature) {

		List<Schema.Field> fields = new ArrayList<>();
		/*
		 * A feature consists and an identifier and a list of properties
		 */
		String id = feature.get("id").getAsString();

		JsonArray properties = feature.get("properties").getAsJsonArray();
		Iterator<JsonElement> iter = properties.iterator();

		while (iter.hasNext()) {

			JsonObject property = iter.next().getAsJsonObject();

			String name = property.get("name").getAsString();
			String type = property.get("type").getAsString();

			JsonElement value = property.get("value");

			String fieldName = id + "_" + name;
			Schema fieldSchema = getFieldSchema(type, value);

			if (fieldSchema != null) {
				Schema.Field field = Schema.Field.of(fieldName, fieldSchema);
				fields.add(field);
			}

		}

		return fields;

	}

	protected Schema getFieldSchema(String fieldType, JsonElement value) {

		Schema schema;

		switch (fieldType) {
		case "NULL": {
			schema = Schema.of(Schema.Type.NULL);
			break;
		}
		case "BOOLEAN": {
			schema = Schema.of(Schema.Type.BOOLEAN);
			break;
		}
		case "DOUBLE": {
			schema = Schema.of(Schema.Type.DOUBLE);
			break;
		}
		case "INTEGER": {
			schema = Schema.of(Schema.Type.INT);
			break;
		}
		case "LONG": {
			schema = Schema.of(Schema.Type.LONG);
			break;
		}
		case "STRING": {
			schema = Schema.of(Schema.Type.STRING);
			break;
		}
		case "ARRAY": {
			/*
			 * An ARRAY property consists of JsonObject items, each with the same structure
			 * of fields, 'type' and 'value'
			 */
			JsonObject item = value.getAsJsonArray().get(0).getAsJsonObject();

			String itemType = item.get("type").getAsString();
			JsonElement itemValue = item.get("value");

			schema = Schema.arrayOf(getFieldSchema(itemType, itemValue));

		}
		case "OBJECT": {
			/*
			 * AN OBJECT property consists of a JsonObject
			 */
			List<Schema.Field> fields = new ArrayList<>();

			JsonObject item = value.getAsJsonObject();
			for (Entry<String, JsonElement> entry : item.entrySet()) {

				String entryName = entry.getKey();
				JsonObject entryValue = entry.getValue().getAsJsonObject();
				/*
				 * Value is an object with two fields, 'type' and 'value'
				 */
				Schema.Field field = Schema.Field.of(entryName,
						getFieldSchema(entryValue.get("type").getAsString(), entryValue.get("value")));
				fields.add(field);

			}

			schema = Schema.recordOf(UUID.randomUUID().toString(), fields);

		}
		default:
			schema = null;
		}

		return schema;

	}

}
