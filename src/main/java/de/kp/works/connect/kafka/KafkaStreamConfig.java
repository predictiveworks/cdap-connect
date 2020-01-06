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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.format.RecordFormats;
import co.cask.hydrator.common.KeyValueListParser;
import co.cask.hydrator.common.ReferencePluginConfig;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Conf for Kafka streaming source.
 */
@SuppressWarnings("unused")
public class KafkaStreamConfig extends StreamConfig {

	private static final String NAME_SCHEMA = "schema";
	private static final String NAME_BROKERS = "brokers";
	private static final String NAME_PARTITIONS = "partitions";
	private static final String NAME_MAX_RATE = "maxRatePerPartition";
	private static final String NAME_INITIAL_PARTITION_OFFSETS = "initialPartitionOffsets";
	private static final String NAME_TIMEFIELD = "timeField";
	private static final String NAME_FORMAT = "format";
	private static final String SEPARATOR = ":";

	private static final long serialVersionUID = 8069169417140954175L;

	@Description(KafkaConstants.KAFKA_TOPIC_READ)
	@Macro
	private String topic;

	@Description(KafkaConstants.KAFKA_TOPIC_PARTITIONS)
	@Nullable
	@Macro
	private String partitions;
	/*
	 * This is a more sophisticated technical configuration to 
	 * term which messages should be read.
	 * 
	 * The initial partitions offsets have to be defined as a key1:value1,key2:value2 list
	 */
	@Description("The initial offset for each topic partition. If this is not specified, "
			+ "all partitions will have the same initial offset, which is determined by the defaultInitialOffset property. "
			+ "An offset of -2 means the smallest offset. An offset of -1 means the latest offset. "
			+ "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read.")
	@Nullable
	@Macro
	private String initialPartitionOffsets;

	@Description("The default initial offset for all topic partitions. "
			+ "An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1. "
			+ "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. "
			+ "If you wish to set different initial offsets for different partitions, use the initialPartitionOffsets property.")
	@Nullable
	@Macro
	private Long defaultInitialOffset;

	/*
	 * Additional fields that can be used (optional) to extend
	 * the output schema for the Kafka message 
	 */
	@Description("Optional name of the field containing the read time of the Kafaka event batch. "
			+ "If this is not set, a time field names '_timestamp' will be added to output records. "
			+ "If set, this field must be present in the schema property and must be a long.")
	@Nullable
	private String timeField;

	@Description("Max number of records to read per second per partition. 0 means there is no limit. Defaults to 1000.")
	@Nullable
	private Integer maxRatePerPartition;
	
	public KafkaStreamConfig() {

		defaultInitialOffset = -1L;
		maxRatePerPartition = 1000;

	}

	public String getTopic() {
		return topic;
	}

	@Nullable
	public String getTimeField() {
		/*
		 * Each event is enriched by a time field, that contains
		 * the read time of the Kafka event batch; the field name
		 * is either provided by the user or it is set internal
		 */
		return Strings.isNullOrEmpty(timeField) ? "_timestamp" : timeField;
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
	 * @param partitionsToRead
	 *            the partitions to read
	 * @param collector
	 *            failure collector
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

	public void validate() {
		/*
		 * Note, macro enabled means that the brokers field
		 * may be empty
		 */
		if (!Strings.isNullOrEmpty(brokers)) {
			getBrokerMap();
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
