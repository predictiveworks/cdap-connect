package de.kp.works.connect.iot.mqtt;
/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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
import java.util.stream.Stream;

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

public class MqttConfig extends BaseMqttConfig implements Serializable {

	private static final long serialVersionUID = 1193310212809609121L;
	
	@Description("The address of the MQTT broker to connect to, including protocol and port.")
	@Macro
	public String mqttBroker;

	@Description("The comma-separated list of MQTT topics to listen to.")
	@Macro
	public String mqttTopics;

	@Description("The format of the MQTT messages to listen to.")
	@Macro
	public String mqttFormat;	
	
	@Override
	public void validate() {
		super.validate();

		String className = this.getClass().getName();
		
		if (Strings.isNullOrEmpty(mqttBroker)) {
			throw new IllegalArgumentException(
					String.format("[%s] The MQTT broker address must not be empty.", className));
		}

		if (Strings.isNullOrEmpty(mqttTopics)) {
			throw new IllegalArgumentException(
					String.format("[%s] The MQTT topics must not be empty.", className));
		}
		
		/* Validate quthentication */
		
		/* Validate MQTT format */
		
		MqttFormat format = getFormat();
		switch(format) {
		case TTN_UPLINK: {

			String[] topics = getTopics();
			for (int i = 0; i < topics.length; i++) {
				
				String[] tokens = topics[i].split("\\/");
				if (tokens.length != 4)
					throw new IllegalArgumentException(
							String.format("[%s] The topics are not compliant to retrieve TTN Uplink messages.", className));
			
				if (tokens[3].equals("up") == false)
					throw new IllegalArgumentException(
							String.format("[%s] The topics are not compliant to retrieve TTN Uplink messages.", className));
			
			}
			
			break;

		}
		default:
			break;
		}

	}
	
	public MqttFormat getFormat() {
		
		Class<MqttFormat> enumClazz = MqttFormat.class;

		return Stream.of(enumClazz.getEnumConstants())
				.filter(keyType -> keyType.getValue().equalsIgnoreCase(mqttFormat)).findAny()
				.orElseThrow(() -> new IllegalArgumentException(
						String.format("Unsupported value for message format: '%s'", mqttFormat)));

	}
	
	public String[] getTopics() {
		
		String[] tokens = mqttTopics.split(",");
		String[]topics = new String[tokens.length];
		
		for (int i = 0; i < tokens.length; i++) {
			topics[i] = tokens[i].trim();
		}
		
		return topics;
		
	}
}
