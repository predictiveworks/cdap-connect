package de.kp.works.connect.zeek.kafka;
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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import de.kp.works.connect.common.BaseConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.plugin.common.KeyValueListParser;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KafkaConfig extends BaseConfig implements Serializable {

	private static final long serialVersionUID = -2437491949830025432L;
	/**
	 * 
	 * KAFKA CONNECTION PARAMETERS
	 * 
	 */
	@Description(KafkaConstants.KAFKA_BROKERS)
	@Macro
	protected String brokers;

	@Description(KafkaConstants.KAFKA_CONSUMER_PROPS)
	@Macro
	@Nullable
	protected String kafkaProperties;

	/**
	 * 
	 * KAFKA STREAM PARAMETERS
	 * 
	 *****/

	@Description(KafkaConstants.KAFKA_TOPIC_READ)
	@Macro
	protected String topic;

	@Description(KafkaConstants.KAFKA_MESSAGE_FORMAT)
	@Nullable
	protected String messageFormat;

	@Description(KafkaConstants.KAFKA_TOPIC_PARTITIONS)
	@Nullable
	@Macro
	protected String partitions;

	@Description(KafkaConstants.KAFKA_MAX_RATE_PER_PARTITION)
	@Nullable
	protected Integer maxRatePerPartition;

	/*
	 * This is a more sophisticated technical configuration to term which messages
	 * should be read.
	 * 
	 * The initial partitions offsets have to be defined as a
	 * key1:value1,key2:value2 list
	 */
	@Description(KafkaConstants.KAFKA_PARTITION_OFFSETS)
	@Nullable
	@Macro
	protected String initialPartitionOffsets;

	@Description(KafkaConstants.KAFKA_INITIAL_OFFSETS)
	@Nullable
	@Macro
	protected Long defaultInitialOffset;

	/**
	 * 
	 * KAFKA OUTPUT EXTENSIONS
	 * 
	 *****/

	/*
	 * Additional fields that can be used (optional) to extend the output schema for
	 * the Kafka message
	 */
	@Description(KafkaConstants.KAFKA_TIME_FIELD)
	@Nullable
	protected String timeField;

	/**
	 * 
	 * KAFKA SECURITY PARAMETERS
	 * 
	 *****/

	@Description(KafkaConstants.KAFKA_PRINCIPAL)
	@Macro
	@Nullable
	protected String principal;

	@Description(KafkaConstants.KAFKA_KEYTAB)
	@Macro
	@Nullable
	protected String keytabLocation;

	public KafkaConfig() {

		defaultInitialOffset = -1L;
		maxRatePerPartition = 1000;

	}

	public String getBrokers() {
		return brokers;
	}
	
	public void getBrokerMap() {

		Map<String, Integer> brokerMap = new HashMap<>();
		try {

			for (KeyValue<String, String> broker : KeyValueListParser.DEFAULT.parse(brokers)) {

				String host = broker.getKey();
				String port = broker.getValue();

				try {

					brokerMap.put(host, Integer.parseInt(port));

				} catch (NumberFormatException e) {
					throw new IllegalArgumentException(String.format("Invalid port '%s' for host '%s'.", port, host));
				}
			}
		} catch (IllegalArgumentException ignored) {
		}

		if (brokerMap.isEmpty()) {
			throw new IllegalArgumentException(String.format("[%s] Kafka brokers must be provided in host:port format.",
					this.getClass().getName()));
		}

	}

	/**
	 * Read additional Kafka consumer properties from the provided input string
	 */
	public Map<String, String> getKafkaProperties() {

		KeyValueListParser parser = new KeyValueListParser("\\s*,\\s*", ":");
		Map<String, String> properties = new HashMap<>();

		if (!Strings.isNullOrEmpty(kafkaProperties)) {
			for (KeyValue<String, String> keyVal : parser.parse(kafkaProperties)) {
				properties.put(keyVal.getKey(), keyVal.getValue());
			}
		}

		return properties;

	}

	public String getTopic() {
		return topic;
	}

	public String getFormat() {
		return Strings.isNullOrEmpty(messageFormat) ? "generic" : messageFormat;
	}

	/**
	 * @return set of partitions to read from. Returns an empty list if no
	 *         partitions were specified.
	 */
	public Set<Integer> getPartitions() {

		Set<Integer> partitionSet = new HashSet<>();
		if (Strings.isNullOrEmpty(partitions)) {
			return partitionSet;
		}

		for (String partition : Splitter.on(',').trimResults().split(partitions)) {
			try {
				partitionSet.add(Integer.parseInt(partition));
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(String.format("Invalid partition '%s'.", partition));
			}
		}

		return partitionSet;

	}

	@Nullable
	public Integer getMaxRatePerPartition() {
		return maxRatePerPartition;
	}

	/**
	 * Get the initial partition offsets for the specified partitions. If an initial
	 * offset is specified in the initialPartitionOffsets property, that value will
	 * be used. Otherwise, the defaultInitialOffset will be used.
	 *
	 * @param partitionsToRead the partitions to read
	 * @return initial partition offsets.
	 */
	public Map<TopicPartition, Long> getInitialPartitionOffsets(Set<Integer> partitionsToRead) {
		Map<TopicPartition, Long> partitionOffsets = new HashMap<>();

		// set default initial partitions
		for (Integer partition : partitionsToRead) {
			partitionOffsets.put(new TopicPartition(topic, partition), defaultInitialOffset);
		}

		// if initial partition offsets are specified, overwrite the defaults.
		if (initialPartitionOffsets != null) {
			for (KeyValue<String, String> partitionAndOffset : KeyValueListParser.DEFAULT
					.parse(initialPartitionOffsets)) {
				String partitionStr = partitionAndOffset.getKey();
				String offsetStr = partitionAndOffset.getValue();
				int partition;
				try {
					partition = Integer.parseInt(partitionStr);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException(
							String.format("Invalid partition '%s' in initialPartitionOffsets.", partitionStr));
				}
				long offset;
				try {
					offset = Long.parseLong(offsetStr);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException(String.format(
							"Invalid offset '%s' in initialPartitionOffsets for partition %d.", offsetStr, partition));
				}
				partitionOffsets.put(new TopicPartition(topic, partition), offset);
			}
		}

		return partitionOffsets;
	}

	@Nullable
	public String getPrincipal() {
		return principal;
	}

	@Nullable
	public String getKeytabLocation() {
		return keytabLocation;
	}

	@Nullable
	public String getTimeField() {
		/*
		 * Each event is enriched by a time field, that contains the read time of the
		 * Kafka event batch; the field name is either provided by the user or it is set
		 * internal
		 */
		return Strings.isNullOrEmpty(timeField) ? "_timestamp" : timeField;
	}

	@Override
	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(brokers)) {
			throw new IllegalArgumentException(
					String.format("[%s] Kafka brokers must not be empty.", this.getClass().getName()));
		}

		getBrokerMap();

		if (Strings.isNullOrEmpty(topic)) {
			throw new IllegalArgumentException(
					String.format("[%s] Kafka topic must not be empty.", this.getClass().getName()));
		}

		Set<Integer> partitions = getPartitions();
		getInitialPartitionOffsets(partitions);

		if (maxRatePerPartition == null) {
			throw new IllegalArgumentException("Max rate per partition must be provided.");
		}

		if (maxRatePerPartition < 0) {
			throw new IllegalArgumentException(String.format("Invalid maxRatePerPartition '%d'.", maxRatePerPartition));
		}

		KafkaHelpers.validateKerberosSetting(principal, keytabLocation);

	}
}
