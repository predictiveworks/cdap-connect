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

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import kafka.api.OffsetRequest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Utility class for Kafka operations
 */
public final class KafkaHelpers {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaHelpers.class);
	public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
	public static final String PRINCIPAL = "principal";
	public static final String KEYTAB = "keytab";

	// This class cannot be instantiated
	private KafkaHelpers() {
	}

	public static <K, V> void validateOffsets(Map<TopicPartition, Long> offsets, Consumer<K, V> consumer) {

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
		
	}
	/**
	 * Fetch the latest offsets for the given topic-partitions
	 *
	 * @param consumer
	 *            The Kafka consumer
	 * @param topicAndPartitions
	 *            topic-partitions to fetch the offsets for
	 * @return Mapping of topic-partiton to its latest offset
	 */
	public static <K, V> Map<TopicPartition, Long> getLatestOffsets(Consumer<K, V> consumer,
			List<TopicPartition> topicAndPartitions) {
		consumer.assign(topicAndPartitions);
		consumer.seekToEnd(topicAndPartitions);

		Map<TopicPartition, Long> offsets = new HashMap<>();
		for (TopicPartition topicAndPartition : topicAndPartitions) {
			long offset = consumer.position(topicAndPartition);
			offsets.put(topicAndPartition, offset);
		}
		return offsets;
	}

	/**
	 * Fetch the earliest offsets for the given topic-partitions
	 *
	 * @param consumer The Kafka consumer
	 * @param topicAndPartitions topic-partitions to fetch the offsets for
	 * @return Mapping of topic-partiton to its earliest offset
	 */
	public static <K, V> Map<TopicPartition, Long> getEarliestOffsets(Consumer<K, V> consumer,
			List<TopicPartition> topicAndPartitions) {
		consumer.assign(topicAndPartitions);
		consumer.seekToBeginning(topicAndPartitions);

		Map<TopicPartition, Long> offsets = new HashMap<>();
		for (TopicPartition topicAndPartition : topicAndPartitions) {
			long offset = consumer.position(topicAndPartition);
			offsets.put(topicAndPartition, offset);
		}
		return offsets;
	}

	/*
	 * Since 0.9.x Kafka supports the following security features:
	 * 
	 * 1. 
	 * 
	 * Authentication of connections to brokers from clients (producers & consumers), 
	 * other brokers and tools, using either SSL or SASL (Kerberos). 
	 * 
	 * SASL/PLAIN can also be used from release 0.10.0.0 onwards
	 * 
	 * 2.
	 * 
	 * Authentication of connections from brokers to ZooKeeper
	 * 
	 * 3. 
	 * 
	 * Encryption of data transferred between brokers and clients, between brokers, 
	 * or between brokers and tools using SSL (Note that there is a performance 
	 * degradation when SSL is enabled, the magnitude of which depends on the CPU 
	 * type and the JVM implementation)
	 * 
	 * 4.
	 * 
	 * Authorization of read / write operations by clients. Authorization is pluggable 
	 * and integration with external authorization services is supported
	 * 
	 * 
	 * Client authentication via SASL
	 * ------------------------------
	 * 
	 * Kafka brokers supports client authentication via SASL. Multiple SASL mechanisms 
	 * can be enabled on the broker simultaneously while each client has to choose one 
	 * mechanism. 
	 * 
	 * The currently supported mechanisms are GSSAPI (Kerberos) and PLAIN.
	 *  
	 */

	
	
	/**
	 * Adds the JAAS conf to the Kafka configuration object for Kafka client login,
	 * if needed. The JAAS conf is not added if either the principal or the keytab
	 * is null.
	 *
	 * @param conf
	 *            Kafka configuration object to add the JAAS conf to
	 * @param principal
	 *            Kerberos principal
	 * @param keytabLocation
	 *            Kerberos keytab for the principal
	 */
	public static void setupKerberosLogin(Map<String, ? super String> conf, @Nullable String principal,
			@Nullable String keytabLocation) {
		if (principal != null && keytabLocation != null) {
			LOG.debug("Adding Kerberos login conf to Kafka for principal {} and keytab {}", principal, keytabLocation);
			
		     conf.put(SASL_JAAS_CONFIG, String.format("com.sun.security.auth.module.Krb5LoginModule required \n" +
                     "        useKeyTab=true \n" +
                     "        storeKey=true  \n" +
                     "        useTicketCache=false  \n" +
                     "        renewTicket=true  \n" +
                     "        keyTab=\"%s\" \n" +
                     "        principal=\"%s\";",
                   keytabLocation, principal));

		} else {
			LOG.debug("Not adding Kerberos login conf to Kafka since either the principal {} or the keytab {} is null",
					principal, keytabLocation);
		}
	}

	/**
	 * Validates whether the principal and keytab are both set or both of them are
	 * null/empty
	 *
	 * @param principal
	 *            Kerberos principal
	 * @param keytab
	 *            Kerberos keytab for the principal
	 */
	public static void validateKerberosSetting(@Nullable String principal, @Nullable String keytab) {
		if (Strings.isNullOrEmpty(principal) != Strings.isNullOrEmpty(keytab)) {
			String emptyField = Strings.isNullOrEmpty(principal) ? PRINCIPAL : KEYTAB;
			String message = emptyField + " is empty. When Kerberos security is enabled for Kafka, "
					+ "then both the principal and the keytab have "
					+ "to be specified. If Kerberos is not enabled, then both should be empty.";
			throw new IllegalArgumentException(message);
		}
	}
}
