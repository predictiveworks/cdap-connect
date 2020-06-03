package de.kp.works.connect.kafka;

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

import java.util.Arrays;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.streaming.StreamingContext;

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
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.kp.works.connect.EmptyFunction;

final class KafkaStreamUtil extends BaseKafkaStreamUtil {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamUtil.class);

	static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context, KafkaConfig config) {

		JavaInputDStream<ConsumerRecord<byte[], byte[]>> stream = createStream(context.getSparkStreamingContext(),
				context.getPipelineName(), config);

		return stream.transform(new RecordTransform(config));

	}
	/**
	 * __KUP__
	 * 
	 * This method is resonsible for extracting the schema associated
	 * from the provided examples; note, the current implementation
	 * restricted the JSON strings to single line strings.
	 */
	private static StructType inferSchema(List<String> samples, Map<String,String> options) {
		
		JsonOptions jsonOptions = new JsonOptions(options, "UTC", "");
		JsonInferSchema inferSchema = new JsonInferSchema(jsonOptions);

		return inferSchema.inferFromJava(samples);

	}

	/**
	 * Applies the format function to each rdd.
	 */
	private static class RecordTransform
			implements Function2<JavaRDD<ConsumerRecord<byte[], byte[]>>, Time, JavaRDD<StructuredRecord>> {

		private static final long serialVersionUID = 8255192804608655854L;
		
		private final KafkaConfig config;
		/*
		 * This variable specifies the output schema that has been inferred
		 * from the incoming JavaRDD batch; note, we determine the data schema
		 * only once, i.e. the first incoming non-zero batch of records makes
		 * the day.
		 */
		private Schema schema;

		RecordTransform(KafkaConfig config) {
			this.config = config;
		}

		@Override
		public JavaRDD<StructuredRecord> call(JavaRDD<ConsumerRecord<byte[], byte[]>> input, Time batchTime) {
			/*
			 * __KUP__
			 * 
			 * This implementation currently expects that the provided batch,
			 * specified as JavaRDD can be collected on the master side:
			 * 
			 * Suppose, we have a network with 1000 nodes, and each node is 
			 * monitored by 10 osqueries (= 10 topics) per second, then this
			 * single topic connector must process 1000 events per second.
			 * 
			 * This should be no huge dataset and collecting it should work.
			 * Otherwise, we break down schema inference to the partitions
			 * of the provided JavaRDD
			 * 
			 */
			List<String> samples = input.map(new StringFunction()).collect();
			if (samples.size() > 0) {
				
				if (schema == null) {
					/*
					 * STEP #1: Infer schema from the extracted sample events
					 */
					Map<String, String> options = new HashMap<String, String>();
					StructType dataType = inferSchema(samples, options);
					/*
					 * STEP #2: Enrich schema with additional fields, that
					 * are outside the incoming event format
					 */
					String timeField = config.getTimeField();

					List<StructField> fields = new ArrayList<StructField>();
					/*
					 * Add the timestamp field to the schema
					 */
					fields.add(new StructField(timeField, DataTypes.LongType, false, Metadata.empty()));
					/*
					 * Add the topic field to the schema
					 */
					fields.add(new StructField("_topic", DataTypes.StringType, false, Metadata.empty()));
					/*
					 * Add the format field to the schema
					 */
					fields.add(new StructField("_format", DataTypes.StringType, false, Metadata.empty()));				
					/*
					 * Add the inferred fields from the message
					 * to the final schema
					 */
					fields.addAll(Arrays.asList(dataType.fields()));

					StructField[] fieldsArray = new StructField[fields.size()];
			        fieldsArray = fields.toArray(fieldsArray);
					
					schema = DataFrames.toSchema(new StructType(fieldsArray));
					/*
					 * Finally publish the derived schema to enable subsequent
					 * schemas to leverage it as input schema 
					 */
					LOG.info("Output schema published: {}", schema);
					
				}
				
				Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> recordFunction = new FormatFunction(
						batchTime.milliseconds(), config, schema);

				return input.map(recordFunction);

			} else {
				return input.map(new EmptyFunction());
			}

		}
	}

	private static class StringFunction implements Function<ConsumerRecord<byte[], byte[]>, String> {

		private static final long serialVersionUID = -663831718518836814L;

		@Override
		public String call(ConsumerRecord<byte[], byte[]> in) throws Exception {

			String line = new String(in.value());
			return line;

		}
		
	}
	
	/**
	 * Common logic for transforming kafka key, message, partition, and offset into
	 * a structured record. Everything here should be serializable, as Spark
	 * Streaming will serialize all functions.
	 */
	private abstract static class BaseFunction implements Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> {

		private static final long serialVersionUID = -6349050933153626269L;
		private final long ts;

		protected final KafkaConfig config;
		protected Schema schema;

		BaseFunction(long ts, KafkaConfig conf, Schema schema) {

			this.ts = ts;
			this.config = conf;
			this.schema = schema;

		}

		@Override
		public StructuredRecord call(ConsumerRecord<byte[], byte[]> in) throws Exception {

			/*
			 * The output schema is defined and the record builder can be used
			 * to assign values to the record
			 */
			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			/*
			 * Each raw message is enriched by a timestamp and the respective
			 * topic as the topic describes the 'context' of the respective
			 * message
			 */
			builder.set(config.getTimeField(), ts);
			builder.set("_topic", in.topic());
			/*
			 * Add the selected format or a generic one to the message
			 */
			builder.set("_format", config.getFormat());
			/*
			 * Add the payload to the respective message and thereby specify
			 * the fields that must not be used (note, the schema also contains
			 * enrichment fields)
			 */
			Set<String> excludeFields = new HashSet<>();
			excludeFields.add(config.getTimeField());
			
			excludeFields.add("_format");
			excludeFields.add("_topic");
			
			addMessage(builder, in.value(), excludeFields);
			StructuredRecord generic = builder.build();
			/*
			 * This method transforms the extracted record into the specified
			 * output format
			 */
			StructuredRecord transformed = transformRecord(generic, config.getFormat(), excludeFields);
			return transformed;
		}

		protected abstract StructuredRecord transformRecord(StructuredRecord input, String format, Set<String> excludeField);
		
		protected abstract void addMessage(StructuredRecord.Builder builder, byte[] message, Set<String> excludeFields)
				throws Exception;
	}

	/**
	 * Transforms kafka key and message into a structured record when message format
	 * and schema are given. Everything here should be serializable, as Spark
	 * Streaming will serialize all functions.
	 */
	private static class FormatFunction extends BaseFunction {
		
		private static final long serialVersionUID = 3641946374069347123L;
		
		FormatFunction(long ts, KafkaConfig config, Schema schema) {
			super(ts, config, schema);
		}

		@Override
		protected void addMessage(StructuredRecord.Builder builder, byte[] message, Set<String> excludeFields)
				throws Exception {
			
			KafkaMessageUtil.messageToRecord(message, builder, schema, excludeFields);

		}
		/*
         * This method takes a specified input format and flattens the input
         * for subsequent processing, e.g. application of SQL statements or
         * Drools compliant business rules
		 */
		@Override
		protected StructuredRecord transformRecord(StructuredRecord input, String format, Set<String> excludeField) {
			return input;
		}
		
	}

	private KafkaStreamUtil() {
		// no-op
	}
}
