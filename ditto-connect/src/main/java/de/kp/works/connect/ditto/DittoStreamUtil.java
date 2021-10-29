package de.kp.works.connect.ditto;
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

import de.kp.works.stream.ditto.DittoNames;
import de.kp.works.stream.ditto.DittoStream;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DittoStreamUtil {

	static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context, DittoConfig config) {

		Properties properties = config.toProperties();
		StorageLevel storageLevel = StorageLevel.MEMORY_ONLY_SER();

		return DittoStream.createDirectStream(context.getSparkStreamingContext(), properties, storageLevel)
			.transform(new RecordTransform(properties));

	}

	public static class RecordTransform implements Function<JavaRDD<String>, JavaRDD<StructuredRecord>> {

		private static final long serialVersionUID = -4426828579132235453L;

		private final Properties properties;

		public RecordTransform(Properties properties) {
			this.properties = properties;
		}

		@Override
		public JavaRDD<StructuredRecord> call(JavaRDD<String> input) {

			if (input.isEmpty())
				return input.map(new EmptyFunction());

			/*
			 * Transform into structured records depends on the user-specified indicators
			 */
			if (properties.containsKey(DittoNames.DITTO_THING_CHANGES())) {

				String flag = properties.getProperty(DittoNames.DITTO_THING_CHANGES());
				if (flag.equals("true")) {
					return input.map(new ThingTransform(properties));
				}

			}

			if (properties.containsKey(DittoNames.DITTO_FEATURES_CHANGES())) {

				String flag = properties.getProperty(DittoNames.DITTO_FEATURES_CHANGES());
				if (flag.equals("true")) {
					return input.map(new FeaturesTransform(properties));
				}

			}

			if (properties.containsKey(DittoNames.DITTO_FEATURE_CHANGES())) {

				String flag = properties.getProperty(DittoNames.DITTO_FEATURE_CHANGES());
				if (flag.equals("true")) {
					return input.map(new FeatureTransform(properties));
				}

			}

			if (properties.containsKey(DittoNames.DITTO_LIVE_MESSAGES())) {

				String flag = properties.getProperty(DittoNames.DITTO_LIVE_MESSAGES());
				if (flag.equals("true")) {
					return input.map(new MessageTransform());
				}

			}

			return input.map(new EmptyFunction());

		}

	}

	private static class EmptyFunction implements Function<String, StructuredRecord> {

		private static final long serialVersionUID = -2582275414113323812L;

		@Override
		public StructuredRecord call(String in) throws Exception {

			List<Schema.Field> schemaFields = new ArrayList<>();
			Schema schema = Schema.recordOf("emptyOutput", schemaFields);

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			return builder.build();

		}

	}

	private DittoStreamUtil() {
		// no-op
	}

}
