package de.kp.works.connect.iot.hivemq;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.connect.iot.mqtt.BaseMqttConfig;
import de.kp.works.connect.iot.mqtt.MqttVersion;

public class HiveMQConfig extends BaseMqttConfig implements Serializable {

	private static final long serialVersionUID = 3127652226872012920L;

	@Description("The host of the HiveMQ broker.")
	@Macro
	public String mqttHost;

	@Description("The port of the HiveMQ broker.")
	@Macro
	public Integer mqttPort;

	@Description("The MQTT topic to subscribe or publish to.")
	@Macro
	public String mqttTopic;

	@Description("The version of MQTT protocol.")
	@Macro
	public String mqttVersion;

	public MqttVersion getMqttVersion() {

		Class<MqttVersion> enumClazz = MqttVersion.class;

		return Stream.of(enumClazz.getEnumConstants()).filter(keyType -> keyType.getValue().equalsIgnoreCase(mqttVersion))
				.findAny().orElseThrow(() -> new IllegalArgumentException(
						String.format("Unsupported value for MQTT version: '%s'", mqttVersion)));

	}

	public void validate() {
		super.validate();

		String className = this.getClass().getName();
		
		if (Strings.isNullOrEmpty(mqttHost)) {
			throw new IllegalArgumentException(
					String.format("[%s] The MQTT host must not be empty.", className));
		}

		if (mqttPort < 1) {
			throw new IllegalArgumentException(
					String.format("[%s] The MQTT port must be positive.", className));
		}
		
		if (Strings.isNullOrEmpty(mqttTopic)) {
			throw new IllegalArgumentException(
					String.format("[%s] The MQTT topic must not be empty.", className));
		}

	}
}

