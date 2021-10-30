package de.kp.works.connect.fiware;
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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;

import java.util.ArrayList;
import java.util.List;

public class FiwareUtil {

	static String className = FiwareUtil.class.getName();

	static String X_WORKS_SERVICE    	= "x_works_service";
	static String X_WORKS_SERVICE_PATH  = "x_works_service_path";
	static String X_WORKS_EVENT       	= "x_works_event";
	static String X_WORKS_TIMESTAMP 	= "x_works_timestamp";

	public static Schema getSchema(JsonObject jsonObject) throws Exception {

		List<Schema.Field> fields = new ArrayList<>();

		/* name: The name of the query that generated this event. */
		if (jsonObject.get("name") == null)
			throw new Exception(
					String.format("[%s] Osquery events must contain a 'name' field.", className));

		fields.add(Schema.Field.of(X_WORKS_TIMESTAMP, Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of(X_WORKS_SERVICE,  Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of(X_WORKS_SERVICE_PATH, Schema.of(Schema.Type.STRING)));

		fields.add(Schema.Field.of(X_WORKS_EVENT,Schema.of(Schema.Type.STRING)));
		return Schema.recordOf("FiwareSchema", fields);

	}

	public static StructuredRecord toRecord(String event, Schema schema) throws Exception {

		JsonElement jsonElement = JsonParser.parseString(event);
		if (!jsonElement.isJsonObject())
			throw new Exception(
					String.format("[%s] Fiware events must be JSON objects.", className));

		JsonObject eventObj = jsonElement.getAsJsonObject();
		JsonObject recordObj = new JsonObject();
		recordObj.addProperty(X_WORKS_TIMESTAMP,System.currentTimeMillis());

		recordObj.addProperty(X_WORKS_SERVICE, eventObj.get("service").getAsString());
		recordObj.addProperty(X_WORKS_SERVICE_PATH, eventObj.get("servicePath").getAsString());

		JsonElement payload = eventObj.get("payload");
		recordObj.addProperty(X_WORKS_EVENT, payload.toString());

		/* Retrieve structured record */
		String json = recordObj.toString();
		return StructuredRecordStringConverter.fromJsonString(json, schema);

	}

}
