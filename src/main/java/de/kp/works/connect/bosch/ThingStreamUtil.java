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

import com.google.gson.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import de.kp.works.ditto.*;

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
					return input.map(new ThingTransform(properties));
				}

			}

			if (properties.containsKey(DittoUtils.DITTO_FEATURES_CHANGES())) {

				String flag = properties.getProperty(DittoUtils.DITTO_FEATURES_CHANGES());
				if (flag.equals("true")) {
					return input.map(new FeaturesTransform(properties));
				}

			}

			if (properties.containsKey(DittoUtils.DITTO_FEATURE_CHANGES())) {

				String flag = properties.getProperty(DittoUtils.DITTO_FEATURE_CHANGES());
				if (flag.equals("true")) {
					return input.map(new FeatureTransform(properties));
				}

			}

			if (properties.containsKey(DittoUtils.DITTO_LIVE_MESSAGES())) {

				String flag = properties.getProperty(DittoUtils.DITTO_LIVE_MESSAGES());
				if (flag.equals("true")) {
					return input.map(new MessageTransform(properties));
				}

			}

			return input.map(new EmptyFunction());

		}

	}

	public static class MessageTransform implements Function<String, StructuredRecord> {

		private static final long serialVersionUID = -8859251744707152433L;

		private Properties properties;
		
		public MessageTransform(Properties properties) {
			this.properties = properties;
		}

		@Override
		public StructuredRecord call(String in) throws Exception {
			
			JsonObject json = new Gson().fromJson(in, JsonObject.class);
			
			Schema schema = buildSchema();
			StructuredRecord.Builder builder = StructuredRecord.builder(schema);

			List<Schema.Field> fields = schema.getFields();
			for (Schema.Field field : fields) {

				String fieldName = field.getName();
				Object fieldValue = null;
				
				if (fieldName.equals("timestamp"))
					fieldValue = json.get(fieldName).getAsLong();
				
				else 
					fieldValue = json.get(fieldName).getAsString();
				
				builder.set(fieldName, fieldValue);

			}
			
			return builder.build();

		}
		
		public Properties getProperties() {
			return properties;
		}
		
		/**
		 * A message schema is static (in contrast to change schemas)
		 * and can be predefined
		 */
		private Schema buildSchema() {

			List<Schema.Field> schemaFields = new ArrayList<>();
			
			Schema.Field timestamp = Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG));
			schemaFields.add(timestamp);
			
			Schema.Field name = Schema.Field.of("name", Schema.of(Schema.Type.STRING));
			schemaFields.add(name);
			
			Schema.Field namespace = Schema.Field.of("namespace", Schema.of(Schema.Type.STRING));
			schemaFields.add(namespace);
			
			Schema.Field subject = Schema.Field.of("subject", Schema.of(Schema.Type.STRING));
			schemaFields.add(subject);
			
			Schema.Field payload = Schema.Field.of("payload", Schema.of(Schema.Type.STRING));
			schemaFields.add(payload);
			
			Schema schema = Schema.recordOf("thingMessage", schemaFields);
			return schema;
			
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
