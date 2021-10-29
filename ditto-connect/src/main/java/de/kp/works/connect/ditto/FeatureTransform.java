package de.kp.works.connect.ditto;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import de.kp.works.stream.ditto.DittoNames;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

public class FeatureTransform extends RecordTransform {

	private static final long serialVersionUID = 377949267048805568L;
	private final Boolean isThing;

	public FeatureTransform(Properties properties) {
		super();

		isThing = properties.containsKey(DittoNames.DITTO_THING_ID());

	}

	/***** BUILD RECORD *****/
	
	@Override
	public StructuredRecord call(String in) {

		JsonObject json = new Gson().fromJson(in, JsonObject.class);
		if (isThing) {
			/*
			 * STEP #1: Build schema
			 */
			Schema schema = buildSchema(json);
			/*
			 * STEP #2: Build structured record based on the feature of the selected thing
			 */
			StructuredRecord.Builder builder = StructuredRecord.builder(schema);

			/* timestamp */
			builder.set("timestamp", json.get("timestamp").getAsLong());

			/* feature */
			JsonObject feature = json.get("feature").getAsJsonObject();
			feature2Record(builder, schema, feature);

			return builder.build();

		} else {
			/*
			 * All things and all features are members of the data stream; in this case, we
			 * cannot build a schema that is based on the individual feature.
			 * 
			 * Here, we aggregate all features into a single JSON data datatype
			 */
			Schema schema = buildSchema(MULTI_THING_SCHEMA);
			StructuredRecord.Builder builder = StructuredRecord.builder(schema);

			/* timestamp */
			builder.set("timestamp", json.get("timestamp").getAsLong());

			/* features */
			JsonObject feature = json.get("feature").getAsJsonObject();
			builder.set("feature", feature.toString());

			return builder.build();

		}
	}

	/***** SCHEMA DEFINITION *****/

	private Schema buildSchema(JsonObject json) {

		List<Schema.Field> schemaFields = new ArrayList<>();

		Schema.Field timestamp = Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG));
		schemaFields.add(timestamp);

		JsonObject feature = json.getAsJsonObject("feature");
		/*
		 * A feature is described by one or more properties
		 */
		schemaFields.addAll(feature2Fields(feature));

		return Schema.recordOf("FeatureMessage", schemaFields);

	}

}
