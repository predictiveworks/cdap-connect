package de.kp.works.connect.things.kafka.transform;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.kp.works.connect.common.SchemaUtil;
import de.kp.works.connect.things.kafka.ThingsboardSourceConfig;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ThingsboardTransform implements Function2<JavaRDD<ConsumerRecord<byte[], byte[]>>, Time, JavaRDD<StructuredRecord>> {

	private static final long serialVersionUID = 2636262566451373259L;
	private final ThingsboardSourceConfig config;
	/*
	 * This variable specifies the output schema that has been inferred
	 * from the incoming JavaRDD batch; note, we determine the data schema
	 * only once, i.e. the first incoming non-zero batch of records makes
	 * the day.
	 */
	private Schema schema;
	
	public ThingsboardTransform(ThingsboardSourceConfig config) {
		this.config = config;
	}
	
	@Override
	public JavaRDD<StructuredRecord> call(JavaRDD<ConsumerRecord<byte[], byte[]>> input, Time batchTime) {
		
		if (input.isEmpty())
			return input.map(new EmptyFunction());
		
		if (schema == null) {
			
			List<JsonObject> samples = input.map(new JsonFunction()).collect();
			schema = SchemaUtil.inferSchema(samples, 10);
			
		}
		
		Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> recordTransform = new RecordTransform(
				config, batchTime.milliseconds(), schema);

		return input.map(recordTransform);

	}

	/**
	 * Transforms kafka key and message into a structured record when message format
	 * is not given. Everything here should be serializable, as Spark Streaming will
	 * serialize all functions.
	 */
	private static class EmptyFunction implements Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> {

		private static final long serialVersionUID = -2582275414113323812L;

		@Override
		public StructuredRecord call(ConsumerRecord<byte[], byte[]> in) {

			List<Schema.Field> schemaFields = new ArrayList<>();
			Schema schema = Schema.recordOf("emptyOutput", schemaFields);

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			return builder.build();

		}

	}

	public static class JsonFunction implements Function<ConsumerRecord<byte[], byte[]>, JsonObject> {

		private static final long serialVersionUID = 7359950369681347065L;

		@Override
		public JsonObject call(ConsumerRecord<byte[], byte[]> in) throws Exception {

			String line = new String(in.value(), StandardCharsets.UTF_8);
			JsonElement jsonElement = JsonParser.parseString(line);
			
			if (!jsonElement.isJsonObject())
				throw new Exception(String.format("[%s] Thingsboard messages must be Json objects.", this.getClass().getName()));
			
			return jsonElement.getAsJsonObject();

		}
		
	}
	
	public static class RecordTransform implements Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> {

		private static final long serialVersionUID = 2383360793734551671L;

		protected final ThingsboardSourceConfig config;

		protected Schema schema;

		public RecordTransform(ThingsboardSourceConfig config, Long batchTime, Schema schema) {
			this.config = config;
			this.schema = schema;
		}
		
		@Override
		public StructuredRecord call(ConsumerRecord<byte[], byte[]> input) throws Exception {
			
			String json = new String(input.value(), StandardCharsets.UTF_8);
			JsonElement jsonElement = JsonParser.parseString(json);
			
			if (!jsonElement.isJsonObject())
				throw new Exception(String.format("[%s] Thingsboard messages must be Json objects.", this.getClass().getName()));
						
			/* Retrieve structured record */
			
			JsonObject jsonObject = jsonElement.getAsJsonObject();
			return StructuredRecordStringConverter.fromJsonString(jsonObject.toString(), schema);

		}

	}
}
