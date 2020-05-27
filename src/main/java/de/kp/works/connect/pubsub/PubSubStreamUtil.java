package de.kp.works.connect.pubsub;
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

import com.google.api.client.auth.oauth2.Credential;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.streaming.StreamingContext;

import de.kp.works.connect.core.BaseStreamUtil;
import de.kp.works.stream.pubsub.*;

public class PubSubStreamUtil extends BaseStreamUtil {

	public static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context,
			PubSubConfig pubSubConfig, Map<String,String> pubSubSecure, Schema schema) {

		setSparkStreamingConf(context, getSparkStreamingProperties(pubSubConfig));

		/* Credentials */
		String serviceAccountFilePath = pubSubConfig.getServiceFilePath(pubSubSecure);

		GCPCredentialsProvider provider = new GCPCredentialsProvider(serviceAccountFilePath);
		Credential credential = provider.getCredential();

		/* Project, subscription & topic */
		String project = pubSubConfig.project;
		String subscription = pubSubConfig.subscription;

		String topic = pubSubConfig.topic;

		JavaDStream<PubSubResult> stream = null;
		if (topic == null) {
			stream = PubSubUtils.createStream(context.getSparkStreamingContext(), project, subscription, credential);

		} else {
			stream = PubSubUtils.createStream(context.getSparkStreamingContext(), project, subscription, topic, credential);

		}
		
		return stream.transform(new PubSubTransform(schema));

	}

	/*
	 * This method is used to add Spark Streaming specific parameters from
	 * configuration
	 */
	private static Properties getSparkStreamingProperties(PubSubConfig config) {

		Properties properties = new Properties();
		return properties;

	}

}
