package de.kp.works.connect.osquery.pubsub.transform;
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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.google.cloud.Timestamp;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import de.kp.works.stream.pubsub.PubSubResult;

public class DefaultTransform extends PubSubTransform {

	private static final long serialVersionUID = 1211030813867661942L;

	private final Schema schema;
	
	public DefaultTransform(Schema schema) {
		this.schema = schema;
	}
	
	@Override
	public JavaRDD<StructuredRecord> call(JavaRDD<PubSubResult> input) {
		
		if (input.isEmpty())
			return input.map(new EmptyPubSubTransform());
		
		return input.map(new ResultTransform(schema));
		
	}
	
	public static class ResultTransform implements Function<PubSubResult, StructuredRecord> {

		private static final long serialVersionUID = 310105894646337067L;

		private final Schema schema;
		
		public ResultTransform(Schema schema) {
			this.schema = schema;
		}

		@Override
		public StructuredRecord call(PubSubResult in) {
			
		   StructuredRecord.Builder builder = StructuredRecord.builder(schema);
		   
		   /* Timestamp */
		   String publishTime = in.publishTime();
		   Long timestamp = getTimestamp(publishTime).toInstant().toEpochMilli();
		   
		   builder.set("timestamp", timestamp);

		   /* id */
		   String id = in.id();
		   builder.set("id", id);
		   
		   /* Attributes */
		   Map<String, String> attributes = in.attributes();
		   builder.set("attributes", attributes);
		   
		   /* Message */
		   byte[] message = in.data();
		   builder.set("message", message);

		   return builder.build();
		}
		
		private ZonedDateTime getTimestamp(String publishTime) {

			Timestamp timestamp = Timestamp.parseTimestamp(publishTime);
			/*
			 * https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage Google
			 * cloud pubsub message timestamp is in RFC3339 UTC "Zulu" format, accurate to
			 * nanoseconds.
			 */
			Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
			return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));

		}
		
	}

}
