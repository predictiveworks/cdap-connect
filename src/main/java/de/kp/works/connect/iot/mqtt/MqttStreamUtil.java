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

import java.util.Properties;

import org.apache.spark.streaming.api.java.JavaDStream;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.streaming.StreamingContext;

import de.kp.works.stream.creds.*;
import de.kp.works.stream.mqtt.*;

public class MqttStreamUtil extends BaseMqttUtil{

	public static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context, MqttConfig config) {

		setSparkStreamingConf(context, getSparkStreamingProperties(config));
		
		Credentials creds = null;

		MqttAuth auth = config.getAuth();
		switch (auth) {
		case BASIC: {
			creds = new BasicCredentials(config.mqttUser, config.mqttPassword);
			break;
		}
		case SSL: {
			break;
		}
		case X509: {
			creds = new PEMX509Credentials(
					config.mqttUser, 
					config.mqttPassword,    
					config.mqttCaCertFile,
					config.mqttCertFile,
					config.mqttKeyFile, 
					config.mqttKeyPass);
			break;
		}
		
		// TODO SSL
/*
 * class SSLCredentials(  
		val username: String,
		val password: String,

		val keystoreFile: String, 
		val keystoreType: String,
		val keystorePassword: String, 
		val keystoreAlgorithm: String,

		val truststoreFile: String, 
		val truststoreType: String,
		val truststorePassword: String, 
		val truststoreAlgorithm: String, 

		val tlsVersion: String) extends Credentials {
		
 */
		}
		
		String[] topics = config.getTopics();
		JavaDStream<MqttResult> stream = MqttUtils.createStream(context.getSparkStreamingContext(), config.mqttBroker,
				topics, null, creds, true);
		
		return stream.transform(new DefaultTransform(config));
		
	}

	private static Properties getSparkStreamingProperties(MqttConfig config) {
		
		Properties properties = new Properties();
		return properties;
	}
}
