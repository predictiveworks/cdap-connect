package de.kp.works.connect.osquery;
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import de.kp.works.connect.kafka.KafkaHelpers;
import de.kp.works.connect.kafka.KafkaParams;

final class OsqueryStreamUtil {
	
	private static final Logger LOG = LoggerFactory.getLogger(OsqueryStreamUtil.class);

	static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context, OsqueryConfig config) {

		Map<String, Object> kafkaParams =  KafkaParams.buildParams(config, context.getPipelineName());
		Properties properties = KafkaParams.buildProperties(config, kafkaParams);
		
		try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(),
				new ByteArrayDeserializer())) {
			/*
			 * KafkaUtils doesn't understand -1 and -2 as smallest offset and 
			 * latest offset. So we have to replace them with the actual smallest 
			 * and latest.
			 */
			Map<TopicPartition, Long> offsets = config
					.getInitialPartitionOffsets(getPartitions(consumer, config));

			KafkaHelpers.validateOffsets(offsets, consumer);
			LOG.info("Using initial offsets {}", offsets);
			
			return KafkaUtils.createDirectStream(context.getSparkStreamingContext(),
					LocationStrategies.PreferConsistent(), ConsumerStrategies
							.<byte[], byte[]>Subscribe(Collections.singleton(config.getTopic()), kafkaParams, offsets))
					.transform(new OsqueryTransform(config));
		}
		
	}

	private static Set<Integer> getPartitions(Consumer<byte[], byte[]> consumer, OsqueryConfig conf) {
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

}
