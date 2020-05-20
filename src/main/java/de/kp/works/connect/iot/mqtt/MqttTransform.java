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
	protected Schema schema;

	public MqttTransform() {
	}
	
	public abstract Schema inferSchema(List<JsonObject> samples);

	public Schema buildPlainSchema() {

		List<Schema.Field> fields = new ArrayList<>();
		
		fields.add(Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of("seconds", Schema.of(Schema.Type.LONG)));
		
		fields.add(Schema.Field.of("format", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("topic", Schema.of(Schema.Type.STRING)));

		fields.add(Schema.Field.of("digest", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("context", Schema.of(Schema.Type.STRING)));

		fields.add(Schema.Field.of("dimension", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("payload", Schema.of(Schema.Type.STRING)));

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

		private Schema schema;
		private String format;
		
		public MultiTopicTransform(Schema schema, String format) {
			
			this.format = format;
			this.schema = schema;
		}
		
		@Override
		public StructuredRecord call(JsonObject in) throws Exception {

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			
			builder.set("timestamp", in.get("timestamp").getAsLong());
			builder.set("seconds", in.get("seconds").getAsLong());

			builder.set("format", format);
			builder.set("topic", in.get("topic").getAsString());

			builder.set("digest", in.get("digest").getAsString());
			builder.set("context", in.get("context").getAsString());
			
			builder.set("dimension", in.get("dimension").getAsString());

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

			/* Timestamp in milliseconds the MQTT message arrived */
			jsonObject.addProperty("timestamp", in.timestamp());
			
			/* Timestamp in seconds the MQTT message arrived;
			 * this field is introduced to support time bins
			 */
			jsonObject.addProperty("seconds", in.seconds());

			/* The complete topic, i.e. all hierarchies this
			 * MQTT message refers to
			 */
			jsonObject.addProperty("topic", in.topic());
			
			/* digest describes the MD5 digest of topic and
			 * serialized payload and can be used to identify
			 * MQTT messages that contain the same information
			 */
			jsonObject.addProperty("digest", in.digest());
			
			/* context describes the MD5 digest of all topic
			 * levels except the last or lowest one and can
			 * be used to group MQTT messages
			 */
			jsonObject.addProperty("context", in.context());
			
			/* dimension describes the last or lowest topic
			 * level and is excepted to specify the name of
			 * a data field, e.g. temperature, pressure etc.
			 */
			jsonObject.addProperty("dimension", in.dimension());
			
			/* The raw payloaf Array[Byte] is not used here
			 */
			JsonElement jsonElement = new JsonParser().parse( in.json());
			
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
