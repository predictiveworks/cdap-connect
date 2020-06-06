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

import javax.annotation.Nullable;

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import de.kp.works.connect.BaseConfig;

public class PubSubConfig extends BaseConfig {

	private static final long serialVersionUID = -8541035348854053890L;

	private static final String PROJECT_DESC = "Google Cloud Project ID, which uniquely identifies "
			+ "a project. It can be found on the Dashboard in the Google Cloud Platform Console.";

	private static final String SERVICE_FILE_PATH_DESC = "Path on the local file system of the service "
			+ "account key used for authorization. When running on clusters, the file must be present "
			+ "on every node in the cluster.";

	private static final String SUBSCRIPTION_DESC = "Cloud Pub/Sub subscription to read from. If a "
			+ "subscription with the specified name does not exist, it will be automatically created "
			+ "if a topic is specified. Messages published before the subscription was created will " + "not be read.";

	private static final String TOPIC_DESC = "Cloud Pub/Sub topic to create a subscription on. This is "
			+ "only used when the specified subscription does not already exist and needs to be automatically "
			+ "created. If the specified subscription already exists, this value is ignored.";

	@Description(PROJECT_DESC)
	@Macro
	public String project;

	@Description(SERVICE_FILE_PATH_DESC)
	@Macro
	public String serviceFilePath;

	@Description(SUBSCRIPTION_DESC)
	public String subscription;

	@Description(TOPIC_DESC)
	@Macro
	@Nullable
	public String topic;

	public void validate() {
		super.validate();

		String className = this.getClass().getName();
		
		if (Strings.isNullOrEmpty(project)) {
			throw new IllegalArgumentException(
					String.format("[%s] The Googe Cloud project id must not be empty.", className));
		}
		
		if (Strings.isNullOrEmpty(serviceFilePath)) {
			throw new IllegalArgumentException(
					String.format("[%s] The path to the service key must not be empty.", className));
		}
		
		if (Strings.isNullOrEmpty(subscription)) {
			throw new IllegalArgumentException(
					String.format("[%s] The Google PubSub subscription must not be empty.", className));
		}
		
	}
	
	public String getServiceFilePath(List<SecureStoreMetadata> secureData) {
		
		// TODO retrieve parameters from secureData		
		return serviceFilePath;
		
	}
}
