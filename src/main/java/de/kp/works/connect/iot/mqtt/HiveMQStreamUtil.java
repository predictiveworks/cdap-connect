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

import java.util.Map;
import java.util.Properties;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.storage.StorageLevel;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.streaming.StreamingContext;

import de.kp.works.stream.mqtt.HiveMQUtils;
import de.kp.works.stream.mqtt.MqttResult;
import de.kp.works.stream.ssl.*;

public class HiveMQStreamUtil extends BaseMqttUtil {

	static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context,HiveMQConfig mqttConfig, Map<String, String> mqttSecure) {

		setSparkStreamingConf(context, getSparkStreamingProperties(mqttConfig));		
		SSLOptions sslOptions = mqttConfig.getMqttSsl(mqttSecure);
		
		int qos = mqttConfig.getMqttQoS().ordinal();		
		int version = 0;
		
		MqttVersion mqttVersion = mqttConfig.getMqttVersion();
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
		
		JavaDStream<MqttResult> stream = HiveMQUtils.createStream(
				context.getSparkStreamingContext(), 
				StorageLevel.MEMORY_AND_DISK_SER_2(), 
				mqttConfig.mqttTopic,
				mqttConfig.mqttHost,
				mqttConfig.mqttPort,
				mqttConfig.mqttUser, 
				mqttConfig.mqttPass, 
				sslOptions, 
				qos,
				version);

		String format = "default";
		String[] topics = new String[] {mqttConfig.mqttTopic};
		
		return stream.transform(new DefaultTransform(format, topics));
	
	}
	/*
	 * This method is used to add Spark Streaming specific
	 * parameters from configuration
	 */
	private static Properties getSparkStreamingProperties(HiveMQConfig config) {
		
		Properties properties = new Properties();
		return properties;
		
	}

}
