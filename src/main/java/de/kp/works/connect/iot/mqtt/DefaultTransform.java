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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.google.gson.JsonObject;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import de.kp.works.stream.mqtt.*;

public class DefaultTransform extends MqttTransform {

	private static final long serialVersionUID = 5511944788990345893L;
	
	public DefaultTransform(MqttConfig config) {
		super(config);
	}
	
	@Override
	public JavaRDD<StructuredRecord> call(JavaRDD<MqttResult> input) throws Exception {
		
		if (input.isEmpty())
			return input.map(new EmptyMqttTransform());

		/*
		 * We transform [MqttResult] into generic [JsonObjects]
		 * and filter those that are NULL
		 */
		JavaRDD<JsonObject> json = input.map(new JsonTransform()).filter(new JsonFilter());
		if (json.isEmpty())
			return json.map(new EmptyJsonTransform());

		/*
		 * Distinguish between a multi topic and a single topic use 
		 * case; for multiple topics, we cannot expect a detailed schema
		 */
		String[] topics = config.getTopics();
		if (topics.length == 1) {

			if (schema == null)
				schema = inferSchema(json.collect(), config);
			
			return json.map(new SingleTopicTransform(schema, config));
			
		} else {

			if (schema == null)
				schema = buildPlainSchema();
			
			return json.map(new MultiTopicTransform(schema, config));
		}

	}
	
	@Override
	public Schema inferSchema(List<JsonObject> samples, MqttConfig config) {
		return DefaultUtil.getSchema(samples);
	}
	
	public class SingleTopicTransform implements Function<JsonObject, StructuredRecord> {

		private static final long serialVersionUID = 1811675023332143555L;

		private MqttConfig config;
		private Schema schema;
		
		public SingleTopicTransform(Schema schema, MqttConfig config) {
			this.config = config;
			this.schema = schema;
		}

		@Override
		public StructuredRecord call(JsonObject in) throws Exception {
			
			JsonObject jsonObject = DefaultUtil.buildJsonObject(in, schema, config);
			
			/* Retrieve structured record */
			String json = jsonObject.toString();
			return StructuredRecordStringConverter.fromJsonString(json, schema);
		}
		
	}

}
