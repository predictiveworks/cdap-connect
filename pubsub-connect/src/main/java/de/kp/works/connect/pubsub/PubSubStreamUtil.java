package de.kp.works.connect.pubsub;
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

import com.google.api.client.auth.oauth2.Credential;
import de.kp.works.connect.pubsub.transform.DefaultTransform;
import de.kp.works.stream.pubsub.GCPCredentialsProvider;
import de.kp.works.stream.pubsub.PubSubResult;
import de.kp.works.stream.pubsub.PubSubStream;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.List;
import java.util.Properties;
import java.util.Set;

public class PubSubStreamUtil {

	public static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context,
			PubSubConfig pubSubConfig, List<SecureStoreMetadata> pubSubSecure, Schema schema) {

		setSparkStreamingConf(context, getSparkStreamingProperties(pubSubConfig));

		/* Credentials */
		String serviceAccountFilePath = pubSubConfig.getServiceFilePath(pubSubSecure);

		GCPCredentialsProvider provider = new GCPCredentialsProvider(serviceAccountFilePath);
		Credential credential = provider.getCredential();

		/* Project, subscription & topic */
		String project = pubSubConfig.project;
		String subscription = pubSubConfig.subscription;

		String topic = pubSubConfig.topic;

		JavaDStream<PubSubResult> stream;
		if (topic == null) {
			stream = PubSubStream.createDirectStream(
					context.getSparkStreamingContext(), project, subscription, credential);

		} else {
			stream = PubSubStream.createDirectStream(
					context.getSparkStreamingContext(), project, subscription, topic, credential);

		}
		
		return stream.transform(new DefaultTransform(schema));

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

	/*
	 * This method is used to add Spark Streaming specific parameters from
	 * configuration
	 */
	private static Properties getSparkStreamingProperties(PubSubConfig config) {
		return new Properties();
	}

}
