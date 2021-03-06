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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import de.kp.works.stream.mqtt.MqttEvent;

/**
 * This transformer supports The Things Network uplink messages, 
 * i.e. topics look like <app-id>/devices/<dev-id>/up
 */
public class TTNTransform extends MqttTransform {

	private static final long serialVersionUID = 693735165758844114L;

	/* The specification of the payload fields */
	private List<String> columns = null;
	private MqttConfig config;
	
	public TTNTransform(MqttConfig config) {
		this.config = config;

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
		 * Distinguish between a multi topic and a single
		 * topic use case; for multiple topics, we cannot
		 * expect a detailed schema
		 */
		String[] topics = config.getTopics();
		if (topics.length == 1) {

			if (schema == null) {
				
				schema = inferSchema(json.collect());
				columns = TTNUtil.getColumns(schema);
				
			}
			
			return json.map(new SingleTopicTransform(schema, columns));
			
		} else {

			if (schema == null)
				schema = buildPlainSchema();
			
			String format = config.getFormat().name().toLowerCase();
			return json.map(new MultiTopicTransform(schema, format));

		}
	
	}

	@Override
	public Schema inferSchema(List<JsonObject> samples) {
		return TTNUtil.getSchema(samples);
	}
	
	public class SingleTopicTransform implements Function<JsonObject, StructuredRecord> {

		private static final long serialVersionUID = 5408293859618821904L;

		/* The specification of the payload fields */
		private List<String> columns = null;
		private Schema schema;
		
		public SingleTopicTransform(Schema schema, List<String> columns) {
			this.columns = columns;
			this.schema = schema;
		}

		@Override
		public StructuredRecord call(JsonObject in) throws Exception {
			
			JsonObject outObject = TTNUtil.buildJsonObject(in, columns);
			
			/* Retrieve structured record */
			String json = outObject.toString();
			return StructuredRecordStringConverter.fromJsonString(json, schema);

		}
		
	}
	
}
