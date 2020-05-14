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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import de.kp.works.stream.mqtt.*;

public class MqttTransform implements Function<JavaRDD<MqttResult>, JavaRDD<StructuredRecord>> {

	private static final long serialVersionUID = 5511944788990345893L;

	private MqttConfig config;
	private Schema schema;
	
	public MqttTransform(MqttConfig config) {
		this.config = config;
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

			if (schema == null)
				schema = inferSchema(input.collect(), config);
			
			return input.map(new SingleTopicTransform(schema, config));
			
		} else {

			if (schema == null) {

				List<Schema.Field> fields = new ArrayList<>();
				
				Schema.Field topic = Schema.Field.of("topic", Schema.of(Schema.Type.STRING));
				fields.add(topic);

				Schema.Field payload = Schema.Field.of("payload", Schema.of(Schema.Type.STRING));
				fields.add(payload);

				schema = Schema.recordOf("topicsOutput", fields);
				
			}
			
			return input.map(new MultiTopicTransform(schema, config));
		}

	}
	
	private Schema inferSchema(List<MqttResult> samples, MqttConfig config) {
		// TODO
		return null;
	}
	
	public class SingleTopicTransform implements Function<MqttResult, StructuredRecord> {

		private static final long serialVersionUID = 1811675023332143555L;

		private MqttConfig config;
		private Schema schema;
		
		public SingleTopicTransform(Schema schema, MqttConfig config) {
			this.config = config;
			this.schema = schema;
		}

		@Override
		public StructuredRecord call(MqttResult in) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	/**
	 * This method transforms a stream of multiple topics into a more
	 * or less generic record format; this approach leaves room for
	 * subsequent pipeline stages to filter and process topic specific
	 * data
	 */
	public class MultiTopicTransform implements Function<MqttResult, StructuredRecord> {

		private static final long serialVersionUID = 2437381949864484498L;

		private MqttConfig config;
		private Schema schema;
		
		public MultiTopicTransform(Schema schema, MqttConfig config) {
			this.config = config;
			this.schema = schema;
		}
		
		@Override
		public StructuredRecord call(MqttResult in) throws Exception {

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			
			// TODO field message format
			builder.set("topic", in.topic());
			builder.set("payload", in.payload());
			
			return builder.build();
			
		}

	}
	/**
	 * This method transforms an empty stream batch into
	 * a dummy structured record
	 */
	public class EmptyTransform implements Function<MqttResult, StructuredRecord> {

		private static final long serialVersionUID = -2582275414113323812L;

		@Override
		public StructuredRecord call(MqttResult in) throws Exception {

			List<Schema.Field> schemaFields = new ArrayList<>();
			Schema schema = Schema.recordOf("emptyOutput", schemaFields);

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			return builder.build();
			
		}

	}

}
