package de.kp.works.connect.iot.ditto;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import de.kp.works.ditto.DittoUtils;

/*
 * This class transforms all changes of all things available; this indicates 
 * that we have to process a variety of variety of different feature sets.
 * 
 */
public class ThingTransform extends RecordTransform {

	private static final long serialVersionUID = -8700001898164977790L;
	private Boolean isThing = false;

	public ThingTransform(Properties properties) {
		super();

		isThing = properties.containsKey(DittoUtils.DITTO_THING_ID());

	}

	/***** BUILD RECORD *****/
	
	@Override
	public StructuredRecord call(String in) throws Exception {

		JsonObject json = new Gson().fromJson(in, JsonObject.class);
		if (isThing) {
			/*
			 * STEP #1: Build schema
			 */
			Schema schema = buildSchema(json);
			/*
			 * STEP #2: Build structured record based on the features of the selected thing
			 */
			StructuredRecord.Builder builder = StructuredRecord.builder(schema);

			/* timestamp */
			builder.set("timestamp", json.get("timestamp").getAsLong());

			/* name */
			builder.set("name", json.get("name").getAsString());

			/* name */
			builder.set("namespace", json.get("namespace").getAsString());

			/* features */
			JsonArray features = json.get("features").getAsJsonArray();
			features2Record(builder, schema, features);

			return builder.build();

		} else {
			/*
			 * All things and all features are members of the data stream; in this case, we
			 * cannot build a schema that is based on the individual feature.
			 * 
			 * Here, we aggregate all features into a single MAP data datatype
			 */
			Schema schema = buildSchema(MULTI_THING_SCHEMA);
			StructuredRecord.Builder builder = StructuredRecord.builder(schema);

			/* timestamp */
			builder.set("timestamp", json.get("timestamp").getAsLong());

			/* name */
			builder.set("name", json.get("name").getAsString());

			/* name */
			builder.set("namespace", json.get("namespace").getAsString());

			/* features */
			JsonArray features = json.get("features").getAsJsonArray();
			builder.set("features", features.toString());

			return builder.build();

		}
	}

	/***** SCHEMA DEFINITION *****/

	private Schema buildSchema(JsonObject json) {

		List<Schema.Field> schemaFields = new ArrayList<>();

		Schema.Field timestamp = Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG));
		schemaFields.add(timestamp);

		Schema.Field name = Schema.Field.of("name", Schema.of(Schema.Type.STRING));
		schemaFields.add(name);

		Schema.Field namespace = Schema.Field.of("namespace", Schema.of(Schema.Type.STRING));
		schemaFields.add(namespace);

		/*
		 * Features are present and represent an Array[JsonObject]
		 */
		JsonArray features = json.getAsJsonArray("features");

		Iterator<JsonElement> iter = features.iterator();
		while (iter.hasNext()) {
			/*
			 * A feature is described by one or more properties
			 */
			JsonObject feature = iter.next().getAsJsonObject();
			schemaFields.addAll(feature2Fields(feature));

		}

		Schema schema = Schema.recordOf("thingMessage", schemaFields);
		return schema;

	}

}
