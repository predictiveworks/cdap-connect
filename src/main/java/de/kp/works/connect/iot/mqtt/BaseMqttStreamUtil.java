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

import co.cask.cdap.etl.api.streaming.StreamingContext;
import de.kp.works.connect.core.BaseStreamUtil;
import de.kp.works.stream.mqtt.MqttResult;
import de.kp.works.stream.mqtt.MqttUtils;
import de.kp.works.stream.ssl.SSLOptions;

public class BaseMqttStreamUtil extends BaseStreamUtil {
	
	protected static JavaDStream<MqttResult> createStream(StreamingContext context, MqttConfig mqttConfig, Map<String,String> mqttSecure) {

		setSparkStreamingConf(context, getSparkStreamingProperties(mqttConfig));		
		SSLOptions sslOptions = mqttConfig.getMqttSsl(mqttSecure);
		
		String[] topics = mqttConfig.getTopics();
		int qos = mqttConfig.getMqttQoS().ordinal();
		
		JavaDStream<MqttResult> stream = MqttUtils.createStream(context.getSparkStreamingContext(), mqttConfig.mqttBroker,
				topics, mqttConfig.mqttUser, mqttConfig.mqttPass, sslOptions, null, true, qos);
		
		return stream;
		
	}
	
	/*
	 * This method is used to add Spark Streaming specific
	 * parameters from configuration
	 */
	protected static Properties getSparkStreamingProperties(MqttConfig config) {
		
		Properties properties = new Properties();
		return properties;
		
	}

}
