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

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import de.kp.works.stream.mqtt.*;

public class DefaultTransform extends MqttTransform {

	private static final long serialVersionUID = 5511944788990345893L;
	
	private String format;
	private String[] topics;
	
	public DefaultTransform(String format, String[] topics) {
		super();
		
		this.format = format;
		this.topics = topics;
		
	}
	
	@Override
	public JavaRDD<StructuredRecord> call(JavaRDD<MqttEvent> input) throws Exception {
		
		if (input.isEmpty())
			return input.map(new EmptyMqttTransform());

		/*
		 * We transform [MqttEvent] into generic [JsonObjects]
		 * and filter those that are NULL
		 */
		JavaRDD<JsonObject> json = input.map(new JsonTransform()).filter(new JsonFilter());
		if (json.isEmpty())
			return json.map(new EmptyJsonTransform());

		/*
		 * Distinguish between a multi topic and a single topic use 
		 * case; for multiple topics, we cannot expect a detailed schema
		 */
		String action = topics2Action(topics);
		if (action.equals("single")) {
			/*
			 * Retrieve a single topic without any wildcards
			 */
			JavaRDD<JsonObject> filtered = json.filter(new SingleTopicFilter());
			if (filtered.isEmpty())
				return filtered.map(new EmptyJsonTransform());
				
			if (schema == null)
				schema = inferSchema(filtered.collect());
			
			return filtered.map(new SingleTopicTransform(schema, format));
			
		} else {

			if (schema == null)
				schema = buildPlainSchema();
			
			return json.map(new MultiTopicTransform(schema, format));
		}

	}
	
	private String topics2Action(String[] topics) {

		String action = "single";

		if (topics.length > 1) {
			action = "multpile";
		} else {
			/* Check for wildcards */
			String[] tokens = topics[0].split("\\/");
			for (int i = 0; i < tokens.length; i++) {

				String token = tokens[i];
				if (token.equals("+") || token.equals("#")) {
					action = "multiple";
					break;
				}
			}
		}
		
		return action;
	}
	
	@Override
	public Schema inferSchema(List<JsonObject> samples) {
		return DefaulSingleTopicUtil.getSchema(samples);
	}
	
	public class SingleTopicTransform implements Function<JsonObject, StructuredRecord> {

		private static final long serialVersionUID = 1811675023332143555L;

		private String format;
		private Schema schema;
		
		public SingleTopicTransform(Schema schema, String format) {
			this.format = format;
			this.schema = schema;
		}

		@Override
		public StructuredRecord call(JsonObject in) throws Exception {
			
			JsonObject jsonObject = DefaulSingleTopicUtil.buildJsonObject(in, schema, format);
			
			/* Retrieve structured record */
			String json = jsonObject.toString();
			return StructuredRecordStringConverter.fromJsonString(json, schema);
		}
		
	}
	/*
	 * The current implementation does to support any structured
	 * payloads 
	 */
	public class SingleTopicFilter implements Function<JsonObject, Boolean> {

		private static final long serialVersionUID = 4723281489022361128L;

		public SingleTopicFilter() {}

		@Override
		public Boolean call(JsonObject in) throws Exception {
			
			JsonElement payload = in.get("payload");
			if (payload.isJsonArray()) {
				/*
				 * We accept payloads that specifies Arrays of basic data types; 
				 * the data type is from the first item 
				 */
				JsonArray array = payload.getAsJsonArray();
				if (array.size() == 0)
					/* Must be delegated to object processing */
					return true;

				JsonElement item = array.get(0);
				return item.isJsonPrimitive();
				
			}
			else if (payload.isJsonObject()) {
				/*
				 * We accept payloads that specify Objects of basic data types
				 * or items that describe Arrays of primitive data types
				 */
				boolean accepted = true;
				for (Map.Entry<String, JsonElement> item : payload.getAsJsonObject().entrySet()) {
					
					JsonElement value = item.getValue();
					if (value.isJsonArray()) {
						
						JsonArray array = value.getAsJsonArray();
						if (array.size() == 0) break;
						
						accepted = array.get(0).isJsonPrimitive();
						if (!accepted) break;
						
					}
					else if (value.isJsonObject()) {
						accepted = false;
						break;
					}
					
				}
				
				return accepted;
			}
			
			else if (payload.isJsonNull()) return false;
			
			else if (payload.isJsonPrimitive()) return true;
			
			return true;

		}
		
	}

}
