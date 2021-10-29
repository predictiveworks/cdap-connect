package de.kp.works.connect.osquery.pubsub;
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

import java.util.List;
import java.util.Properties;
import java.util.Set;

import de.kp.works.connect.osquery.pubsub.transform.FleetTransform;
import de.kp.works.stream.pubsub.PubSubStream;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.google.api.client.auth.oauth2.Credential;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import de.kp.works.stream.pubsub.GCPCredentialsProvider;
import de.kp.works.stream.pubsub.PubSubResult;

public class FleetStreamUtil {

	public static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context,
			FleetConfig fleetConfig, List<SecureStoreMetadata> fleetSecure) {

		setSparkStreamingConf(context, getSparkStreamingProperties(fleetConfig));

		/* Credentials */
		String serviceAccountFilePath = fleetConfig.getServiceFilePath(fleetSecure);

		GCPCredentialsProvider provider = new GCPCredentialsProvider(serviceAccountFilePath);
		Credential credential = provider.getCredential();

		/* Project, subscription & topic */
		String project = fleetConfig.project;
		String subscription = fleetConfig.subscription;

		String topic = fleetConfig.topic;

		JavaDStream<PubSubResult> stream;
		if (topic == null) {
			stream = PubSubStream.createDirectStream(context.getSparkStreamingContext(), project, subscription, credential);

		} else {
			stream = PubSubStream.createDirectStream(context.getSparkStreamingContext(), project, subscription, topic, credential);

		}
		
		return stream.transform(new FleetTransform());

	}

	/*
	 * This method is used to add Spark Streaming specific parameters from
	 * configuration
	 */
	private static Properties getSparkStreamingProperties(FleetConfig config) {
		return new Properties();
	}

	private static void setSparkStreamingConf(StreamingContext context, Properties properties) {

		org.apache.spark.streaming.StreamingContext ssc = context.getSparkStreamingContext().ssc();
		Set<Object> keys = properties.keySet();

		for (Object key: keys) {

			String k = (String)key;
			String v = properties.getProperty(k);
			ssc.conf().set(k, v);

		}

	}

}
