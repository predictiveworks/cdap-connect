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

	public static final String KAFKA_TOPIC_READ = "The Kafka topic from which messages are read.";

	public static final String KAFKA_TOPIC_PARTITIONS = "The topic partitions to read from. If not specified, all partitions will be read.";
	
	public static final String KAFKA_PARTITION_OFFSETS = "The initial offset for each topic partition. If this is not specified, "
			+ "all partitions will have the same initial offset, which is determined by the 'Initial Offset' property. An offset "
			+ "of -2 means the smallest offset. An offset of -1 means the latest offset. Offsets are inclusive. If an offset "
			+ "of 5 is used, the message at offset 5 will be read.";

	public static final String KAFKA_INITIAL_OFFSETS = "The default initial offset for all topic partitions. "
			+ "An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1. "
			+ "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. If you wish "
			+ "to set different initial offsets for different partitions, use the 'Partition Offsets' property.";

	public static final String KAFKA_MAX_RATE_PER_PARTITION = "Max number of records to read per second per partition. "
			+ "0 means there is no limit. Defaults to 1000.";
	
	public static final String KAFKA_PRINCIPAL = "The Kerberos principal used when Kerberos security is enabled for Kafka.";

	public static final String KAFKA_KEYTAB = "The keytab location for the Kerberos principal when Kerberos security is enabled for Kafka.";

	public static final String KAFKA_TIME_FIELD = "Optional name of the field containing the read time of the Kafaka event batch. "
			+ "If this is not set, a time field names '_timestamp' will be added to output records. "
			+ "If set, this field must be present in the schema property and must be a long.";
	
	public static final String KAFKA_MESSAGE_FORMAT = "Optional application level format of the Kafka events. If no format is provided, generic "
			+ "inference of the data schema is provided and no application specific processing is done.";

}
