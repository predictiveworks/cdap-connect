package de.kp.works.connect.hivemq;
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

import de.kp.works.stream.mqtt.hivemq.HiveStream;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HiveStreamUtil {
	/*
	 * mqttSecure is currently unused
	 */
	public static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(
			StreamingContext context, HiveSourceConfig hiveConfig, List<SecureStoreMetadata> mqttSecure) {

		Properties properties = hiveConfig.toProperties();
		StorageLevel storageLevel = StorageLevel.MEMORY_ONLY_SER();

		return HiveStream.createDirectStream(context.getSparkStreamingContext(), properties, storageLevel)
				.transform(new RecordTransform());

	}

	public static class RecordTransform implements Function<JavaRDD<String>, JavaRDD<StructuredRecord>> {

		private static final long serialVersionUID = -4426828579132235453L;

		public RecordTransform() {
		}

		@Override
		public JavaRDD<StructuredRecord> call(JavaRDD<String> input) {

			if (input.isEmpty())
				return input.map(new EmptyFunction());

			return input.map(new MqttTransform());

		}

	}

	private static class EmptyFunction implements Function<String, StructuredRecord> {

		private static final long serialVersionUID = -2582275414113323812L;

		@Override
		public StructuredRecord call(String in) {

			List<Schema.Field> schemaFields = new ArrayList<>();
			Schema schema = Schema.recordOf("emptyOutput", schemaFields);

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			return builder.build();

		}

	}

	private HiveStreamUtil() {
		// no-op
	}
}
