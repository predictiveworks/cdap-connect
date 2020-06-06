package de.kp.works.connect.kafka.osquery;
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import de.kp.works.connect.EmptyFunction;

/*
 * [OsqueryTransform] is the main class for transforming
 * Apache Kafka messages originating from osquery endpoint
 * monitor 
 */
public class OsqueryTransform
		implements Function2<JavaRDD<ConsumerRecord<byte[], byte[]>>, Time, JavaRDD<StructuredRecord>> {

	private static final long serialVersionUID = -2256941762899970287L;

	@SuppressWarnings("unused")
	private OsqueryConfig config;
	private Schema schema;

	public OsqueryTransform(OsqueryConfig config) {
		this.config = config;
	}

	@Override
	public JavaRDD<StructuredRecord> call(JavaRDD<ConsumerRecord<byte[], byte[]>> input, Time batchTime)
			throws Exception {

		if (input.isEmpty())
			return input.map(new EmptyFunction());

		if (schema == null) {
			schema = getSchema(input.first());
		}
		/*
		 * Schema strategy: The schema is inferred from the first record and then
		 * assigned to the event transformer;
		 * 
		 * this is a suitable strategy as the [osquery] schema is more or less static
		 * due to its strong relationship to predefined queries.
		 */
		Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> logTransform = new LogTransform(
				batchTime.milliseconds(), schema);

		return input.map(logTransform);

	}

	private Schema getSchema(ConsumerRecord<byte[], byte[]> record) throws Exception {

		String event = new String(record.value(), "UTF-8");
		JsonElement jsonElement = new JsonParser().parse(event);

		if (!jsonElement.isJsonObject())
			throw new Exception(
					String.format("[%s] Osquery events must be JSON objects.", OsqueryTransform.class.getName()));

		JsonObject eventObject = jsonElement.getAsJsonObject();
		return OsqueryUtil.getSchema(eventObject);

	}
}
