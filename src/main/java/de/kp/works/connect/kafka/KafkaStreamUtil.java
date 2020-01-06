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
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import kafka.api.OffsetRequest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

final class KafkaStreamUtil {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamUtil.class);

	static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context, KafkaStreamConfig config) {

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());

		/* Spark saves the offsets in checkpoints, no need for Kafka to save them */
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		
		kafkaParams.put("key.deserializer", ByteArrayDeserializer.class.getCanonicalName());
		kafkaParams.put("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
		
		KafkaHelpers.setupKerberosLogin(kafkaParams, config.getPrincipal(), config.getKeytabLocation());
		/*
		 * Create a unique string for the group.id using the pipeline name and the topic;
		 * group.id is a Kafka consumer property that uniquely identifies the group of 
		 * consumer processes to which this consumer belongs.
		 */
		kafkaParams.put("group.id", Joiner.on("-").join(context.getPipelineName().length(), config.getTopic().length(),
				context.getPipelineName(), config.getTopic()));

		kafkaParams.putAll(config.getKafkaProperties());

		Properties properties = new Properties();
		properties.putAll(kafkaParams);
		/*
		 * The default session.timeout.ms = 30000 (30s) and the fetch.max.wait.ms = 500 (0.5s);
		 * KafkaConsumer checks whether smaller than session timeout or fetch timeout; in this
		 * case an exception is thrown.
		 */
		int requestTimeout = 30 * 1000 + 1000;
		if (config.getKafkaProperties().containsKey(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG)) {
			requestTimeout = Math.max(requestTimeout,
					Integer.valueOf(config.getKafkaProperties().get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) + 1000));
		}
		properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
		
		properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
		
		try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(),
				new ByteArrayDeserializer())) {

			Map<TopicPartition, Long> offsets = config
					.getInitialPartitionOffsets(getPartitions(consumer, config));

			// KafkaUtils doesn't understand -1 and -2 as smallest offset and latest offset.
			// so we have to replace them with the actual smallest and latest
			List<TopicPartition> earliestOffsetRequest = new ArrayList<>();
			List<TopicPartition> latestOffsetRequest = new ArrayList<>();
			for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
				TopicPartition topicAndPartition = entry.getKey();
				Long offset = entry.getValue();
				if (offset == OffsetRequest.EarliestTime()) {
					earliestOffsetRequest.add(topicAndPartition);
				} else if (offset == OffsetRequest.LatestTime()) {
					latestOffsetRequest.add(topicAndPartition);
				}
			}

			Set<TopicPartition> allOffsetRequest = Sets
					.newHashSet(Iterables.concat(earliestOffsetRequest, latestOffsetRequest));
			Map<TopicPartition, Long> offsetsFound = new HashMap<>();
			offsetsFound.putAll(KafkaHelpers.getEarliestOffsets(consumer, earliestOffsetRequest));
			offsetsFound.putAll(KafkaHelpers.getLatestOffsets(consumer, latestOffsetRequest));

			for (TopicPartition topicAndPartition : allOffsetRequest) {
				offsets.put(topicAndPartition, offsetsFound.get(topicAndPartition));
			}

			Set<TopicPartition> missingOffsets = Sets.difference(allOffsetRequest, offsetsFound.keySet());
			if (!missingOffsets.isEmpty()) {
				throw new IllegalStateException(String.format(
						"Could not find offsets for %s. Please check all brokers were included in the broker list.",
						missingOffsets));
			}
			LOG.info("Using initial offsets {}", offsets);
			
			return KafkaUtils.createDirectStream(context.getSparkStreamingContext(),
					LocationStrategies.PreferConsistent(), ConsumerStrategies
							.<byte[], byte[]>Subscribe(Collections.singleton(config.getTopic()), kafkaParams, offsets))
					.transform(new RecordTransform(config));
		}
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
		
		private final KafkaStreamConfig config;
		/*
		 * This variable specifies the output schema that has been inferred
		 * from the incoming JavaRDD batch; note, we determine the data schema
		 * only once, i.e. the first incoming non-zero batch of records makes
		 * the day.
		 */
		private Schema schema;

		RecordTransform(KafkaStreamConfig config) {
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

	private static Set<Integer> getPartitions(Consumer<byte[], byte[]> consumer, KafkaStreamConfig conf) {
		Set<Integer> partitions = conf.getPartitions();

		if (!partitions.isEmpty()) {
			return partitions;
		}

		partitions = new HashSet<>();
		for (PartitionInfo partitionInfo : consumer.partitionsFor(conf.getTopic())) {
			partitions.add(partitionInfo.partition());
		}
		return partitions;
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

		protected final KafkaStreamConfig config;
		protected Schema schema;

		BaseFunction(long ts, KafkaStreamConfig conf, Schema schema) {

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
	 * is not given. Everything here should be serializable, as Spark Streaming will
	 * serialize all functions.
	 */
	private static class EmptyFunction implements Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> {

		private static final long serialVersionUID = -2582275414113323812L;

		@Override
		public StructuredRecord call(ConsumerRecord<byte[], byte[]> in) throws Exception {

			List<Schema.Field> schemaFields = new ArrayList<>();
			Schema schema = Schema.recordOf("emptyOutput", schemaFields);

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			return builder.build();
			
		}

	}

	/**
	 * Transforms kafka key and message into a structured record when message format
	 * and schema are given. Everything here should be serializable, as Spark
	 * Streaming will serialize all functions.
	 */
	private static class FormatFunction extends BaseFunction {
		
		private static final long serialVersionUID = 3641946374069347123L;
		
		FormatFunction(long ts, KafkaStreamConfig config, Schema schema) {
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
			// TODO
			return input;
		}
		
	}

	private KafkaStreamUtil() {
		// no-op
	}
}
