package de.kp.works.connect.osquery.kafka.transform;
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

import java.util.HashSet;
import java.util.Set;

import de.kp.works.connect.osquery.kafka.KafkaConfig;
import de.kp.works.connect.osquery.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

public class EventTransform implements Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> {

	private final long ts;

	protected final KafkaConfig config;
	protected Schema schema;

	public EventTransform(long ts, KafkaConfig conf, Schema schema) {

		this.ts = ts;
		this.config = conf;
		this.schema = schema;

	}

	@Override
	public StructuredRecord call(ConsumerRecord<byte[], byte[]> in) {

		/*
		 * The output schema is defined and the record builder can be used
		 * to assign values to the record
		 */
		StructuredRecord.Builder builder = StructuredRecord.builder(schema);
		/*
		 * Each raw message is enriched by a timestamp and the respective
		 * topic as the topic describes the 'context' of the respective
		 * message
		 */
		builder.set(config.getTimeField(), ts);
		builder.set("topic", in.topic());
		/*
		 * Add the payload to the respective message and thereby specify
		 * the fields that must not be used (note, the schema also contains
		 * enrichment fields)
		 */
		Set<String> excludeFields = new HashSet<>();
		excludeFields.add(config.getTimeField());
		excludeFields.add("topic");
		
		KafkaUtil.messageToRecord(in.value(), builder, schema, excludeFields);
		return builder.build();

	}

}
