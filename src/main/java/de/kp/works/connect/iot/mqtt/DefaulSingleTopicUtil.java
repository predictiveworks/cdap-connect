package de.kp.works.connect.iot.mqtt;
/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.connect.core.SchemaUtil;

public class DefaulSingleTopicUtil implements Serializable {

	private static final long serialVersionUID = 1839935244896054913L;

	/***** SCHEMA *****/

	public static Schema getSchema(List<JsonObject> samples) {
		return getSchema(samples, 10);
	}

	private static Schema getSchema(List<JsonObject> objects, Integer samples) {

		int size = objects.size();
		Random random = new Random();

		/* The final inferred schema */
		Schema result = null;
		
		/* Randomly selected samples to infer a schema */
		
		String topic = null;
		String[] tokens = null;
		
		for (int i = 0; i < samples; i++) {

			int index = random.nextInt(size);
			JsonObject obj = objects.get(index);
			
			if (topic == null) {
				
				topic = obj.get("topic").getAsString();
				tokens = topic.split("\\/");
				
			}

			JsonElement payload = objects.get(index).get("payload");			
			if (payload.isJsonArray()) {
				
				/* Json Array
				 * 
				 * The topic is expected to describe a single field
				 * and its value is assumed to be an Array of basic
				 * data values.
				 * 
				 * In case of an array result, we not not infer the 
				 * schema from the set of samples
				 */
				if (result != null) break;
				
				JsonArray array = payload.getAsJsonArray();
				if (array.size() == 0) continue;
				
				Schema schema = getArraySchema(array, tokens);
				if (schema != null)
					result = schema;

			}
			
			else if (payload.isJsonObject()) {

				/* Json Object
				 * 
				 * The topic is expected to describe a single field
				 * and its value is assumed to be an object of basic
				 * data values.
				 * 
				 * In case of an object result, we not not infer the 
				 * schema from the set of samples
				 */
				if (result != null) break;
				
				JsonObject object = payload.getAsJsonObject();
				Schema schema = null;
				
				schema = getObjectSchema(object, tokens);
				if (schema != null)
					result = schema;

			}
			
			else if (payload.isJsonPrimitive()) {
				
				/* Json Primitive
				 * 				 
				 * The topic is expected to describe a single field
				 * and its value is assumed to be a basicdata value.
				 * 
				 * In case of a primitive result, we not notinfer the 
				 * schema from the set of samples
				 */
				if (result != null) break;
				
				Schema schema = getPrimitiveSchema(payload.getAsJsonPrimitive(), tokens);
				if (schema != null)
					result = schema;
				
			}
			
		}
		
		if (result == null)
			throw new RuntimeException("Schema inference failed for a single fully qualified MQTT topic");
		
		return result;
		
	}
	
	private static Schema getArraySchema(JsonArray array, String[] tokens) {

		JsonPrimitive primitive = array.get(0).getAsJsonPrimitive();		
		Schema fieldSchema = SchemaUtil.primitive2Schema(primitive);
		
		if (fieldSchema == null)
			return null;
		
		else {
			
			List<Schema.Field> fields = new ArrayList<>();

			/* Common fields & topic extraction */
			fields.addAll(getCommonFields(tokens));
			
			/*
			 * The last topic level is used to infer the
			 * field name
			 */
			String fieldName = tokens[tokens.length - 1];			
			/* 
			 * In order to increase flexibility, we define
			 * a nullable schema for the MQTT field
			 */
			fields.add(Schema.Field.of(fieldName, Schema.nullableOf(Schema.arrayOf(fieldSchema))));
			
			Schema schema = Schema.recordOf("defaultSchema", fields);
			return schema;
			
			
		}
	}
	
	private static Schema getObjectSchema(JsonObject object, String[] tokens) {

		Schema schema = null;
		Boolean isAccepted = true;

		/*
		 * It is an optimistic approach to retrieve the schema
		 * from the object
		 */
		List<Schema.Field> fields = new ArrayList<>();

		/* Common fields & topic extraction */
		fields.addAll(getCommonFields(tokens));
		
		/*
		 * The last topic level is used to infer the
		 * field name
		 */
		String fieldName = tokens[tokens.length - 1];
		for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
			
			String itemName = fieldName + "_" + entry.getKey();
			JsonElement entryValue = entry.getValue();
			/*
			 * We expect that the entry is a [JsonPrimitive] and
			 * also no [JsonNull]
			 */
			if (entryValue.isJsonNull() || entryValue.isJsonObject()) {

				isAccepted = false;
				break;

			}
			
			Schema itemSchema = null;
			if (entryValue.isJsonArray()) {
				
				JsonElement entryElem = entryValue.getAsJsonArray().get(0);
				
				Schema elemSchema = SchemaUtil.primitive2Schema(entryElem.getAsJsonPrimitive());
				if (elemSchema == null) {
					
					isAccepted = false;
					break;
	
				}
				
				itemSchema = Schema.arrayOf(elemSchema);
				
			}
			else {

				itemSchema = SchemaUtil.primitive2Schema(entryValue.getAsJsonPrimitive());
				if (itemSchema == null) {
	
					isAccepted = false;
					break;
	
				}
			
			}
			/* 
			 * In order to increase flexibility, we define
			 * a nullable schema for the MQTT field
			 */
			fields.add(Schema.Field.of(itemName, Schema.nullableOf(itemSchema)));
			
		}
		
		if (isAccepted)
			schema = Schema.recordOf("defaultSchema", fields);
		
		return schema;
		
	}
	
	private static Schema getPrimitiveSchema(JsonPrimitive primitive, String[] tokens) {
		
		Schema fieldSchema = SchemaUtil.primitive2Schema(primitive);
		if (fieldSchema == null)
			return null;
		
		else {
			
			List<Schema.Field> fields = new ArrayList<>();

			/* Common fields & topic extraction */
			fields.addAll(getCommonFields(tokens));
			
			/*
			 * The last topic level is used to infer the
			 * field name
			 */
			String fieldName = tokens[tokens.length - 1];
			/* 
			 * In order to increase flexibility, we define
			 * a nullable schema for the MQTT field
			 */
			fields.add(Schema.Field.of(fieldName, Schema.nullableOf(fieldSchema)));
			
			Schema schema = Schema.recordOf("defaultSchema", fields);
			return schema;
			
			
		}

	}

	private static List<Schema.Field> getCommonFields(String[] tokens) {
		
		List<Schema.Field> fields = new ArrayList<>();

		/* Common fields */
		fields.add(Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)));		
		
		fields.add(Schema.Field.of("format", Schema.of(Schema.Type.STRING)));			
		fields.add(Schema.Field.of("topic", Schema.of(Schema.Type.STRING)));			
		
		/* Topic extraction */		
		if (tokens.length > 1) {
			/*
			 * The tokens are added as contextual information 
			 * to enable subsequent fitering or more
			 */
			for (int i = 0; i < tokens.length -1; i++) {
				
				String fieldName = "level_" + i;
				fields.add(Schema.Field.of(fieldName, Schema.of(Schema.Type.STRING)));
				
			}
		}

		return fields;
		
	}
	
	private static JsonObject setCommonFields(JsonObject out, JsonObject in, String format, String topic, String[] tokens) {
		
		out.add("timestamp", in.get("timestamp"));
		
		out.addProperty("format", format);
		out.addProperty("topic", topic);
		
		if (tokens.length > 1) {
			/*
			 * The tokens are added as contextual information 
			 * to enable subsequent fitering or more
			 */
			for (int i = 0; i < tokens.length -1; i++) {
				
				String fieldName = "level_" + i;
				String fieldValue = tokens[i];
				
				out.addProperty(fieldName, fieldValue);;
				
			}
		}

		return out;
	}
	
	/***** JSON OBJECT *****/

	public static JsonObject buildJsonObject(JsonObject in, Schema schema, MqttConfig config) throws Exception {

		JsonObject out = new JsonObject();

		String format = config.getFormat().name().toLowerCase();
		
		String topic = in.get("topic").getAsString();
		String[] tokens = topic.split("\\/");
		
		/* Common Fields */

		out = setCommonFields(out, in, format, topic, tokens);
		
		/* Payload fields */
		
		/*
		 * The last topic level is used to infer the
		 * field name
		 */
		String fieldName = tokens[tokens.length - 1];
		
		JsonElement payload = in.get("payload");
		if (payload.isJsonArray() || payload.isJsonPrimitive())
			out.add(fieldName, payload);
			
		else if (payload.isJsonObject()) {
			/*
			 * We have to flatten the field names
			 */
			for (Map.Entry<String, JsonElement> entry : payload.getAsJsonObject().entrySet()) {
				
				String itemName = fieldName + "_" + entry.getKey();
				JsonElement itemValue = entry.getValue();

				out.add(itemName, itemValue);
				
			}
			
		}
		
		return out;

	}

}
