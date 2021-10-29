package de.kp.works.connect.osquery.kafka;
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class BaseStreamUtil {

	protected static JavaInputDStream<ConsumerRecord<byte[], byte[]>> createStream(JavaStreamingContext jssc,
			String pipeline, KafkaConfig config) {

		Map<String, Object> params = KafkaParams.buildParams(config, pipeline);
		Properties properties = KafkaParams.buildProperties(config, params);

		try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(),
				new ByteArrayDeserializer())) {
			return createStream(jssc, consumer, config, params);
		}

	}

	private static JavaInputDStream<ConsumerRecord<byte[], byte[]>> createStream(JavaStreamingContext jssc,
			Consumer<byte[], byte[]> consumer, KafkaConfig config, Map<String, Object> params) {
		/*
		 * KafkaUtils doesn't understand -1 and -2 as smallest offset and latest offset.
		 * So we have to replace them with the actual smallest and latest.
		 */
		Map<TopicPartition, Long> offsets = config.getInitialPartitionOffsets(getPartitions(consumer, config));

		KafkaHelpers.validateOffsets(offsets, consumer);

		return KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies
						.Subscribe(Collections.singleton(config.getTopic()), params, offsets));

	}

	protected static Set<Integer> getPartitions(Consumer<byte[], byte[]> consumer, KafkaConfig conf) {

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
