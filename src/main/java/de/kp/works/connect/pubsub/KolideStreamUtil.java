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

import java.util.List;
import java.util.Properties;

import org.apache.spark.streaming.api.java.JavaDStream;

import com.google.api.client.auth.oauth2.Credential;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import de.kp.works.connect.core.BaseStreamUtil;
import de.kp.works.stream.pubsub.GCPCredentialsProvider;
import de.kp.works.stream.pubsub.PubSubResult;
import de.kp.works.stream.pubsub.PubSubUtils;

public class KolideStreamUtil extends BaseStreamUtil {

	public static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context,
			KolideConfig kolideConfig, List<SecureStoreMetadata> kolideSecure) {

		setSparkStreamingConf(context, getSparkStreamingProperties(kolideConfig));

		/* Credentials */
		String serviceAccountFilePath = kolideConfig.getServiceFilePath(kolideSecure);

		GCPCredentialsProvider provider = new GCPCredentialsProvider(serviceAccountFilePath);
		Credential credential = provider.getCredential();

		/* Project, subscription & topic */
		String project = kolideConfig.project;
		String subscription = kolideConfig.subscription;

		String topic = kolideConfig.topic;

		JavaDStream<PubSubResult> stream = null;
		if (topic == null) {
			stream = PubSubUtils.createStream(context.getSparkStreamingContext(), project, subscription, credential);

		} else {
			stream = PubSubUtils.createStream(context.getSparkStreamingContext(), project, subscription, topic, credential);

		}
		
		return stream.transform(new KolideTransform());

	}

	/*
	 * This method is used to add Spark Streaming specific parameters from
	 * configuration
	 */
	private static Properties getSparkStreamingProperties(KolideConfig config) {

		Properties properties = new Properties();
		return properties;

	}

}
