package de.kp.works.connect.osquery.kafka;
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

import de.kp.works.connect.osquery.kafka.transform.OsqueryTransform;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.streaming.StreamingContext;

final class OsqueryStreamUtil extends BaseStreamUtil {

	public static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context,
			OsqueryConfig config) {

		JavaInputDStream<ConsumerRecord<byte[], byte[]>> stream = createStream(context.getSparkStreamingContext(),
				context.getPipelineName(), config);
		return stream.transform(new OsqueryTransform(config));

	}

}
