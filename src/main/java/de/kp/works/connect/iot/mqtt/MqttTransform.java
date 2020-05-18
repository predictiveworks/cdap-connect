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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.stream.mqtt.MqttResult;

public abstract class MqttTransform implements Function<JavaRDD<MqttResult>, JavaRDD<StructuredRecord>> {

	private static final long serialVersionUID = 5511944788990345893L;

	protected Gson GSON = new Gson();
	
	protected MqttConfig config;
	protected Schema schema;

	public MqttTransform(MqttConfig config) {
		this.config = config;
	}
	
	public abstract Schema inferSchema(List<JsonObject> samples, MqttConfig config);

	public Schema buildPlainSchema() {

		List<Schema.Field> fields = new ArrayList<>();
		
		Schema.Field timestamp = Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG));
		fields.add(timestamp);
		
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
	public class MultiTopicTransform implements Function<JsonObject, StructuredRecord> {

		private static final long serialVersionUID = 2437381949864484498L;

		private MqttConfig config;
		private Schema schema;
		
		public MultiTopicTransform(Schema schema, MqttConfig config) {
			this.config = config;
			this.schema = schema;
		}
		
		@Override
		public StructuredRecord call(JsonObject in) throws Exception {

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			
			builder.set("timestamp", in.get("timestamp").getAsLong());
			builder.set("format", config.getFormat().name().toLowerCase());
			
			builder.set("topic", in.get("topic").getAsString());
			
			JsonElement payload = in.get("payload");
			builder.set("payload", GSON.toJson(payload));
			
			return builder.build();
			
		}

	}
	
	/**
	 * This method transforms an empty stream batch into
	 * a dummy structured record
	 */
	public class EmptyMqttTransform implements Function<MqttResult, StructuredRecord> {

		private static final long serialVersionUID = -2582275414113323812L;

		@Override
		public StructuredRecord call(MqttResult in) throws Exception {

			List<Schema.Field> schemaFields = new ArrayList<>();
			Schema schema = Schema.recordOf("mqttSchema", schemaFields);

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			return builder.build();
			
		}

	}
	
	/**
	 * This method transforms an empty stream batch 
	 * into a dummy structured record
	 */
	public class EmptyJsonTransform implements Function<JsonObject, StructuredRecord> {

		private static final long serialVersionUID = -1442067065391093229L;

		@Override
		public StructuredRecord call(JsonObject in) throws Exception {

			List<Schema.Field> schemaFields = new ArrayList<>();
			Schema schema = Schema.recordOf("mqttSchema", schemaFields);

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			return builder.build();
			
		}

	}
	/**
	 * The initial transformation stage to
	 * turn [MqttResult] into [JsonObject]
	 */
	public class JsonTransform implements Function<MqttResult, JsonObject> {
		
		private static final long serialVersionUID = 2152957637609061446L;

		public JsonTransform() {}

		@Override
		public JsonObject call(MqttResult in) throws Exception {
			
			JsonObject jsonObject = new JsonObject();
			
			jsonObject.addProperty("timestamp", in.timestamp());
			jsonObject.addProperty("topic", in.topic());
			
			/* Parse plain byte message */
			Charset UTF8 = Charset.forName("UTF-8");

			String json = new String(in.payload(), UTF8);
			JsonElement jsonElement = new JsonParser().parse(json);
			
			jsonObject.add("payload", jsonElement);
			return jsonObject;

		}
		
	}
	public class JsonFilter implements Function<JsonObject, Boolean> {

		private static final long serialVersionUID = 4723281489022361128L;

		public JsonFilter() {}

		@Override
		public Boolean call(JsonObject in) throws Exception {
			
			JsonElement payload = in.get("payload");
			if (payload.isJsonNull()) return false;
			
			return true;

		}
		
	}

}
