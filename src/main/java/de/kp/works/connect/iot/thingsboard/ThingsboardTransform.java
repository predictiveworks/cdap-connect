package de.kp.works.connect.iot.thingsboard;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import de.kp.works.connect.EmptyFunction;
import de.kp.works.connect.core.SchemaUtil;

public class ThingsboardTransform implements Function2<JavaRDD<ConsumerRecord<byte[], byte[]>>, Time, JavaRDD<StructuredRecord>> {

	private static final long serialVersionUID = 2636262566451373259L;
	private ThingsboardSourceConfig config;
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
	public JavaRDD<StructuredRecord> call(JavaRDD<ConsumerRecord<byte[], byte[]>> input, Time batchTime) throws Exception {
		
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

	public static class JsonFunction implements Function<ConsumerRecord<byte[], byte[]>, JsonObject> {

		private static final long serialVersionUID = 7359950369681347065L;

		@Override
		public JsonObject call(ConsumerRecord<byte[], byte[]> in) throws Exception {

			String line = new String(in.value(), "UTF-8");
			JsonElement jsonElement = new JsonParser().parse(line);
			
			if (!jsonElement.isJsonObject())
				throw new Exception(String.format("[%s] Thingsboard messages must be Json objects.", this.getClass().getName()));
			
			return jsonElement.getAsJsonObject();

		}
		
	}
	
	public static class RecordTransform implements Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> {

		private static final long serialVersionUID = 2383360793734551671L;

		protected final ThingsboardSourceConfig config;	
		@SuppressWarnings("unused")
		private final long batchTime;
		
		protected Schema schema;

		public RecordTransform(ThingsboardSourceConfig config, Long batchTime, Schema schema) {
			this.config = config;
			this.batchTime = batchTime;
			
			this.schema = schema;
		}
		
		@Override
		public StructuredRecord call(ConsumerRecord<byte[], byte[]> input) throws Exception {
			
			String json = new String(input.value(), "UTF-8");
			JsonElement jsonElement = new JsonParser().parse(json);
			
			if (!jsonElement.isJsonObject())
				throw new Exception(String.format("[%s] Thingsboard messages must be Json objects.", this.getClass().getName()));
						
			/* Retrieve structured record */
			
			JsonObject jsonObject = jsonElement.getAsJsonObject();
			return StructuredRecordStringConverter.fromJsonString(jsonObject.toString(), schema);

		}

	}
}
