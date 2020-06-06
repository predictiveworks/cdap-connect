package de.kp.works.connect.iot.hivemq;
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

import java.io.IOException;
import java.util.List;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import de.kp.works.connect.iot.mqtt.MqttVersion;

import de.kp.works.stream.ssl.SSLOptions;
import de.kp.works.stream.mqtt.HiveMQClient;
import de.kp.works.stream.mqtt.HiveMQClientBuilder;

public class HiveMQUtil {

	private HiveMQClient client;

	public HiveMQUtil(HiveMQSinkConfig config, List<SecureStoreMetadata> mqttSecure) {

		SSLOptions sslOptions = config.getMqttSsl(mqttSecure);

		int qos = config.getMqttQoS().ordinal();
		int version = 0;

		MqttVersion mqttVersion = config.getMqttVersion();
		switch (mqttVersion) {
		case V_3: {
			version = 3;
			break;
		}
		case V_5: {
			version = 5;
			break;
		}
		}

		this.client = HiveMQClientBuilder.build(config.mqttTopic, config.mqttHost, config.mqttPort, config.mqttUser,
				config.mqttPass, sslOptions, qos, version);

		this.client.connect();

	}

	public void publish(StructuredRecord record) throws IOException {

		String json = StructuredRecordStringConverter.toJsonString(record);
		this.client.publish(json);
		
	}

}
