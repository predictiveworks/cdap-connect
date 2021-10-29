package de.kp.works.connect.zeek.kafka.transform;
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

import de.kp.works.connect.zeek.kafka.KafkaConfig;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.catalyst.json.JsonInferSchema;
import org.apache.spark.sql.catalyst.json.JsonOptions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Time;

import java.util.*;

public class KafkaTransform
		implements Function2<JavaRDD<ConsumerRecord<byte[], byte[]>>, Time, JavaRDD<StructuredRecord>> {

	private static final long serialVersionUID = 3245612492744637050L;

	private final KafkaConfig config;
	/*
	 * This variable specifies the output schema that has been inferred from the
	 * incoming JavaRDD batch; note, we determine the data schema only once, i.e.
	 * the first incoming non-zero batch of records makes the day.
	 */
	private Schema schema;

	public KafkaTransform(KafkaConfig config) {
		this.config = config;
	}

	@Override
	public JavaRDD<StructuredRecord> call(JavaRDD<ConsumerRecord<byte[], byte[]>> input, Time batchTime) {

		if (input.isEmpty())
			return input.map(new EmptyFunction());
		/*
		 * __KUP__
		 * 
		 * This implementation currently expects that the provided batch, specified as
		 * JavaRDD can be collected on the master side:
		 * 
		 */
		if (schema == null) {

			List<String> samples = input.map(new StringFunction()).collect();
			schema = getSchema(samples);

		}

		return input.map(new EventTransform(batchTime.milliseconds(), config, schema));

	}

	private Schema getSchema(List<String> samples) {
		/*
		 * __KUP__
		 * 
		 * This method is responsible for extracting the schema associated from the
		 * provided examples; note, the current implementation restricted the JSON
		 * strings to single line strings.
		 */
		Map<String, String> options = new HashMap<>();
		JsonOptions jsonOptions = new JsonOptions(options, "UTC", "");

		JsonInferSchema inferSchema = new JsonInferSchema(jsonOptions);
		StructType dataType = inferSchema.inferFromJava(samples);

		/*
		 * Enrich schema with additional fields, that are outside the incoming event
		 * format
		 */
		String timeField = config.getTimeField();

		List<StructField> fields = new ArrayList<>();
		/*
		 * Add the timestamp field to the schema
		 */
		fields.add(new StructField(timeField, DataTypes.LongType, false, Metadata.empty()));
		/*
		 * Add the topic field to the schema
		 */
		fields.add(new StructField("topic", DataTypes.StringType, false, Metadata.empty()));
		/*
		 * Add the inferred fields from the message to the final schema
		 */
		fields.addAll(Arrays.asList(dataType.fields()));

		StructField[] fieldsArray = new StructField[fields.size()];
		fieldsArray = fields.toArray(fieldsArray);

		return DataFrames.toSchema(new StructType(fieldsArray));

	}

	/**
	 * Transforms kafka key and message into a structured record when message format
	 * is not given. Everything here should be serializable, as Spark Streaming will
	 * serialize all functions.
	 */
	private static  class EmptyFunction implements Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> {

		private static final long serialVersionUID = -2582275414113323812L;

		@Override
		public StructuredRecord call(ConsumerRecord<byte[], byte[]> in) {

			List<Schema.Field> schemaFields = new ArrayList<>();
			Schema schema = Schema.recordOf("emptyOutput", schemaFields);

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			return builder.build();

		}

	}

	private static class StringFunction implements Function<ConsumerRecord<byte[], byte[]>, String> {

		private static final long serialVersionUID = -663831718518836814L;

		@Override
		public String call(ConsumerRecord<byte[], byte[]> in) {
			return new String(in.value());
		}

	}

}
