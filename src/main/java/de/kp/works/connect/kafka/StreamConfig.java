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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.hydrator.common.KeyValueListParser;
import co.cask.hydrator.common.ReferencePluginConfig;

public class StreamConfig extends ReferencePluginConfig implements Serializable {

	private static final long serialVersionUID = -74600648183604185L;
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

	/**
	 * 
	 * KAFKA MESSAGE PARAMETERS
	 * 
	 *****/

	@Description("Optional application level format of the Kafka events. If no format is provided, generic "
			+ "inference of the data schema is provided and no application specific processing is done.")
	@Nullable
	protected String format;

	public StreamConfig() {
		super("");
	}

	public String getBrokers() {
		return brokers;
	}
	
	/**
	 * Read additional Kafka consumer properties from
	 * the provided input string
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

	@Nullable
	public String getPrincipal() {
		return principal;
	}

	@Nullable
	public String getKeytabLocation() {
		return keytabLocation;
	}

	@Nullable
	public String getFormat() {
		return Strings.isNullOrEmpty(format) ? "generic" : format;
	}

	/**
	 * @return broker host to broker port mapping.
	 */
	public Map<String, Integer> getBrokerMap() {

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
		} catch (IllegalArgumentException e) {
		}

		if (brokerMap.isEmpty()) {
			throw new IllegalArgumentException("Kafka brokers must be provided in host:port format.");
		}
		return brokerMap;
	}

}
