package de.kp.works.connect.kafka;
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

import com.google.common.base.Joiner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaParams implements Serializable {

	private static final long serialVersionUID = -2958314213413268585L;

	public static Map<String, Object> buildParams(KafkaConfig config, String pipelineName) {

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
		kafkaParams.put("group.id", Joiner.on("-").join(pipelineName.length(), config.getTopic().length(),
				pipelineName, config.getTopic()));

		kafkaParams.putAll(config.getKafkaProperties());

		return kafkaParams;
		
	}

	public static Properties buildProperties(KafkaConfig config, Map<String, Object> kafkaParams) {

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
					Integer.parseInt(config.getKafkaProperties().get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) + 1000));
		}
		
		properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
		properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");

		return properties;
		
	}
	
}
