package de.kp.works.connect.bosch;
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

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.ws.*;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.streaming.StreamingContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

public class ThingStreamUtil {

	static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(StreamingContext context, ThingConfig config) {

		Properties properties = config.getThingConf();
		StorageLevel storageLevel = StorageLevel.MEMORY_ONLY_SER();

		return DittoStreamUtils.createDirectStream(context.getSparkStreamingContext(), properties, storageLevel)
				.transform(new RecordTransform(properties));

	}

	public static class RecordTransform implements Function<JavaRDD<String>, JavaRDD<StructuredRecord>> {

		private static final long serialVersionUID = -4426828579132235453L;

		private Properties properties;

		public RecordTransform(Properties properties) {
			this.properties = properties;
		}

		@Override
		public JavaRDD<StructuredRecord> call(JavaRDD<String> input) throws Exception {

			if (input.isEmpty())
				return input.map(new EmptyFunction());

			/*
			 * Transform into structured records depends on the user-specified indicators
			 */
			if (properties.containsKey(DittoUtils.DITTO_THING_CHANGES())) {

				String flag = properties.getProperty(DittoUtils.DITTO_THING_CHANGES());
				if (flag.equals("true")) {
					return input.map(new ThingFunction());
				}

			}

			if (properties.containsKey(DittoUtils.DITTO_FEATURES_CHANGES())) {

				String flag = properties.getProperty(DittoUtils.DITTO_FEATURES_CHANGES());
				if (flag.equals("true")) {
					return input.map(new FeaturesFunction());
				}

			}

			if (properties.containsKey(DittoUtils.DITTO_FEATURE_CHANGES())) {

				String flag = properties.getProperty(DittoUtils.DITTO_FEATURE_CHANGES());
				if (flag.equals("true")) {
					return input.map(new FeatureFunction());
				}

			}

			if (properties.containsKey(DittoUtils.DITTO_LIVE_MESSAGES())) {

				String flag = properties.getProperty(DittoUtils.DITTO_LIVE_MESSAGES());
				if (flag.equals("true")) {
					return input.map(new MessageFunction());
				}

			}

			return input.map(new EmptyFunction());

		}

	}

	private static class ThingFunction implements Function<String, StructuredRecord> {

		private static final long serialVersionUID = -4241854791385529970L;

		@Override
		public StructuredRecord call(String in) throws Exception {
			// TODO
			return null;
		}

	}

	private static class FeaturesFunction implements Function<String, StructuredRecord> {

		private static final long serialVersionUID = -7865686438932012000L;

		@Override
		public StructuredRecord call(String in) throws Exception {
			// TODO
			return null;
		}

	}

	private static class FeatureFunction implements Function<String, StructuredRecord> {

		private static final long serialVersionUID = 8855558414425001019L;

		@Override
		public StructuredRecord call(String in) throws Exception {
			// TODO
			return null;
		}

	}

	private static class MessageFunction implements Function<String, StructuredRecord> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public StructuredRecord call(String in) throws Exception {
			// TODO
			return null;
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

	private ThingStreamUtil() {
		// no-op
	}

}
