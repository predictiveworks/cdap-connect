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
import de.kp.works.stream.mqtt.MqttResult;

public abstract class MqttTransform implements Function<JavaRDD<MqttResult>, JavaRDD<StructuredRecord>> {

	private static final long serialVersionUID = 5511944788990345893L;

	protected MqttConfig config;
	protected Schema schema;

	public MqttTransform(MqttConfig config) {
		this.config = config;
	}
	
	public abstract Schema inferSchema(List<MqttResult> samples, MqttConfig config);

	public Schema buildPlainSchema() {

		List<Schema.Field> fields = new ArrayList<>();
		
		Schema.Field format = Schema.Field.of("format", Schema.of(Schema.Type.STRING));
		fields.add(format);
		
		Schema.Field topic = Schema.Field.of("topic", Schema.of(Schema.Type.STRING));
		fields.add(topic);

		Schema.Field payload = Schema.Field.of("payload", Schema.of(Schema.Type.STRING));
		fields.add(payload);

		schema = Schema.recordOf("plainSchema", fields);
		return schema;
		
	}
	/**
	 * 
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
			builder.set("format", config.getFormat().name().toLowerCase());
			
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
			Schema schema = Schema.recordOf("mqttSchema", schemaFields);

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			return builder.build();
			
		}

	}

}
