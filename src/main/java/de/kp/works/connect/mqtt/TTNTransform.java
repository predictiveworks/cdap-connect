package de.kp.works.connect.mqtt;
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
import de.kp.works.stream.mqtt.MqttResult;

/**
 * This transformer supports The Things Network uplink messages, 
 * i.e. topics look like <app-id>/devices/<dev-id>/up
 */
public class TTNTransform extends MqttTransform {

	private static final long serialVersionUID = 693735165758844114L;

	/* The specification of the payload fields */
	private List<String> columns = null;
	
	public TTNTransform(MqttConfig config) {
		super(config);

	}

	@Override
	public JavaRDD<StructuredRecord> call(JavaRDD<MqttResult> input) throws Exception {
		
		if (input.isEmpty())
			return input.map(new EmptyTransform());
		
		/*
		 * Distinguish between a multi topic and a single
		 * topic use case; for multiple topics, we cannot
		 * expect a detailed schema
		 */
		String[] topics = config.getTopics();
		if (topics.length == 1) {

			if (schema == null) {
				
				schema = inferSchema(input.collect(), config);
				columns = TTNUtil.getColumns(schema);
				
			}
			
			return input.map(new SingleTopicTransform(schema, columns));
			
		} else {

			if (schema == null)
				schema = buildPlainSchema();
			
			return input.map(new MultiTopicTransform(schema, config));

		}
	
	}

	@Override
	public Schema inferSchema(List<MqttResult> samples, MqttConfig config) {
		return TTNUtil.getSchema(samples);
	}
	
	public class SingleTopicTransform implements Function<MqttResult, StructuredRecord> {

		private static final long serialVersionUID = 5408293859618821904L;

		/* The specification of the payload fields */
		private List<String> columns = null;
		private Schema schema;
		
		public SingleTopicTransform(Schema schema, List<String> columns) {
			this.columns = columns;
			this.schema = schema;
		}

		@Override
		public StructuredRecord call(MqttResult in) throws Exception {
			
			JsonObject jsonObject = TTNUtil.buildJsonObject(in, columns);
			
			/* Retrieve structured record */
			String json = jsonObject.toString();
			return StructuredRecordStringConverter.fromJsonString(json, schema);

		}
		
	}
	
}
