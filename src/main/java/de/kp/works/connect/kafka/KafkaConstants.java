package de.kp.works.connect.kafka;

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

public class KafkaConstants {

	public static final String KAFKA_BROKERS = "Specifies the list of Kafka brokers in host1:port1,host2:port2 form "
			+ "to determine the leader for each topic. For example: host1.example.com:9092,host2.example.com:9092";

	public static final String KAFKA_CONSUMER_PROPS = "Specifies additional Kafka consumer properties to set in form of a key-value list. "
			+ "For example: key1:value1, key2:value2 etc.";

	public static final String KAFKA_FORMAT = "Optional application level format of the Kafka events. If no format is provided, generic "
			+ "inference of the data schema is provided and no application specific processing is done.";

	public static final String KAFKA_PRINCIPAL = "The Kerberos principal used when Kerberos security is enabled for Kafka.";

	public static final String KAFKA_KEYTAB = "The keytab location for the Kerberos principal when Kerberos security is enabled for Kafka.";

	public static final String KAFKA_PRODUCER_PROPS = "Specifies additional Kafka producer properties to set in form of a key-value list. "
			+ "For example: key1:value1, key2:value2 etc.";

	public static final String KAFKA_TOPIC_PARTITIONS = "The topic partitions to read from. If not specified, all partitions will be read. "
			+ "Please provde as a comma-separated list. For example: partition1,partition2 etc.";

	public static final String KAFKA_TOPICS_PARTITIONS = "The partitions for each topic to read from. If not specified, all partitions will be read. "
			+ "Please provide as a comma-separated list. For example: topic1:partition1,topic1:partition2,topic2:partition3 etc.";

	public static final String KAFKA_TOPIC_READ = "The Kafka topic from which messages are read.";

	public static final String KAFKA_TOPICS_READ = "The Kafka topics from which messages are read. Please provided as a comma-separated list. "
			+ "For example: topic1,topic2,topic3";

	public static final String KAFKA_TOPIC_WRITE = "The Kafka topic to which messages need to be published. "
			+ "Note, the topic should already exist. Please the Kafka configuration.";

}
