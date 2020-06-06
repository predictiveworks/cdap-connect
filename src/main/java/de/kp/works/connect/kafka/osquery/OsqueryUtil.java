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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import de.kp.works.connect.core.SchemaUtil;

public class OsqueryUtil {

	/*
	 * { 
	 * 	"name":"pack_it-compliance_alf_explicit_auths",
	 * 	"hostIdentifier":"192-168-0-4.rdsnet.ro",
	 * 	"calendarTime":"Thu Dec 28 14:39:50 2017 UTC", 
	 * 	"unixTime":"1514471990",
	 * 	"epoch":"0", 
	 * 	"counter":"0", 
	 * 	"decorations":{
	 * 		"host_uuid":"4AB2906D-5516-5794-AF54-86D1D7F533F3", 
	 * 		"username":"tsg" 
	 * 	},
	 * 	"columns":{ 
	 * 		"process":"org.python.python.app" 
	 * 	}, 
	 * 	"action":"added" 
	 * }
	 *
	 */
	public static Schema getSchema(JsonObject jsonObject) throws Exception {

		List<Schema.Field> fields = new ArrayList<>();

		/* name: The name of the query that generated this event. */
		if (jsonObject.get("name") == null)
			throw new Exception(
					String.format("[%s] Osquery events must contain a 'name' field.", OsqueryUtil.class.getName()));

		fields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

		/*
		 * action: For incremental data, marks whether the entry was added. or removed.
		 * It can be one of "added", "removed", or "snapshot".
		 */
		if (jsonObject.get("action") == null)
			throw new Exception(
					String.format("[%s] Osquery events must contain an 'action' field.", OsqueryUtil.class.getName()));

		fields.add(Schema.Field.of("action", Schema.of(Schema.Type.STRING)));

		/* columns */
		if (jsonObject.get("columns") == null)
			throw new Exception(
					String.format("[%s] Osquery events must contain columns.", OsqueryUtil.class.getName()));

		JsonObject columns = jsonObject.get("columns").getAsJsonObject();
		fields.addAll(columns2Fields(columns));

		/*
		 * decorations: these originate from the result of decoration query and are
		 * optional
		 */
		if (jsonObject.get("decorations") != null) {

			JsonObject decorations = jsonObject.get("decorations").getAsJsonObject();
			fields.addAll(decorations2Fields(decorations));

		}

		/*
		 * hostIdentifier [optional] The identifier for the host on which the osquery
		 * agent is running. Normally the hostname.
		 */
		if (jsonObject.get("hostIdentifier") != null) {
			fields.add(Schema.Field.of("host_identifier", Schema.of(Schema.Type.STRING)));

		}

		/*
		 * calendarTime [optional] String representation of the collection time, as
		 * formatted by osquery.
		 */
		if (jsonObject.get("calendarTime") != null) {
			fields.add(Schema.Field.of("calendar_time", Schema.of(Schema.Type.STRING)));

		}

		/*
		 * unixTime [optional] Unix timestamp of the event, in seconds since the epoch,
		 * transformed into milliseconds
		 */
		if (jsonObject.get("unixTime") != null) {
			fields.add(Schema.Field.of("unix_time", Schema.of(Schema.Type.LONG)));

		}

		Schema schema = Schema.recordOf("osquery_event", fields);
		return schema;

	}

	private static List<Schema.Field> columns2Fields(JsonObject columns) throws Exception {

		List<Schema.Field> fields = new ArrayList<>();
		for (Entry<String, JsonElement> entry : columns.entrySet()) {

			String entryKey = entry.getKey();
			JsonElement entryValue = entry.getValue();

			if (!entryValue.isJsonPrimitive())
				throw new Exception(
						String.format("[%s] Osquery columns must be primitive types.", OsqueryUtil.class.getName()));

			String fieldName = "column_" + entryKey;
			fields.add(Schema.Field.of(fieldName, SchemaUtil.primitive2Schema(entryValue.getAsJsonPrimitive())));

		}

		return fields;

	}

	private static List<Schema.Field> decorations2Fields(JsonObject decorations) throws Exception {

		List<Schema.Field> fields = new ArrayList<>();
		for (Entry<String, JsonElement> entry : decorations.entrySet()) {

			String entryKey = entry.getKey();
			JsonElement entryValue = entry.getValue();

			if (!entryValue.isJsonPrimitive())
				throw new Exception(String.format("[%s] Osquery decorations must be primitive types.",
						OsqueryUtil.class.getName()));

			String fieldName = "info_" + entryKey;
			fields.add(Schema.Field.of(fieldName, SchemaUtil.primitive2Schema(entryValue.getAsJsonPrimitive())));

		}

		return fields;

	}

	public static StructuredRecord toRecord(byte[] event, Schema schema) throws Exception {

		String json = new String(event, "UTF-8");
		return toRecord(json, schema);

	}

	public static StructuredRecord toRecord(String event, Schema schema) throws Exception {

		JsonElement jsonElement = new JsonParser().parse(event);
		if (!jsonElement.isJsonObject())
			throw new Exception(
					String.format("[%s] Osquery events must be JSON objects.", OsqueryUtil.class.getName()));

		JsonObject eventObject = jsonElement.getAsJsonObject();
		/*
		 * Event object are transformed and renamed before exposed to CDAP pipelines
		 */
		JsonObject jsonObject = new JsonObject();

		/* name */
		jsonObject.add("name", eventObject.get("name"));

		/* action */
		jsonObject.add("action", eventObject.get("action"));

		/* columns */
		jsonObject = columns2Object(jsonObject, eventObject.get("columns").getAsJsonObject());

		/* decorations */
		if (eventObject.get("decorations") != null) {
			jsonObject = decorations2Object(jsonObject, eventObject.get("decorations").getAsJsonObject());

		}

		/*
		 * hostIdentifier [optional] The identifier for the host on which the osquery
		 * agent is running. Normally the hostname.
		 */
		if (eventObject.get("hostIdentifier") != null) {
			jsonObject.add("host_identifier", eventObject.get("hostIdentifier"));
		}

		/*
		 * calendarTime [optional] String representation of the collection time, as
		 * formatted by osquery.
		 */
		if (eventObject.get("calendarTime") != null) {
			jsonObject.add("calendar_time", eventObject.get("calendarTime"));
		}

		/*
		 * unixTime [optional] Unix timestamp of the event, in seconds since the epoch,
		 * transformed into milliseconds
		 */
		if (eventObject.get("unixTime") != null) {

			Long ts = getTimestamp(eventObject.get("unixTime"));
			jsonObject.addProperty("unix_time", ts);

		}

		/* Retrieve structured record */
		String json = jsonObject.toString();
		return StructuredRecordStringConverter.fromJsonString(json, schema);

	}

	private static JsonObject columns2Object(JsonObject jsonObject, JsonObject columns) throws Exception {

		for (Entry<String, JsonElement> entry : columns.entrySet()) {

			String entryKey = entry.getKey();
			JsonElement entryValue = entry.getValue();

			if (!entryValue.isJsonPrimitive())
				throw new Exception(String.format("[%s] Osquery columns must be primitive types.",
						OsqueryUtil.class.getName()));

			String fieldName = "column_" + entryKey;
			jsonObject.add(fieldName, entryValue.getAsJsonPrimitive());
			
		}

		return jsonObject;

	}

	private static JsonObject decorations2Object(JsonObject jsonObject, JsonObject decorations) throws Exception {

		for (Entry<String, JsonElement> entry : decorations.entrySet()) {

			String entryKey = entry.getKey();
			JsonElement entryValue = entry.getValue();

			if (!entryValue.isJsonPrimitive())
				throw new Exception(String.format("[%s] Osquery decorations must be primitive types.",
						OsqueryUtil.class.getName()));

			String fieldName = "info_" + entryKey;
			jsonObject.add(fieldName, entryValue.getAsJsonPrimitive());
			
		}

		return jsonObject;

	}

	private static Long getTimestamp(JsonElement unixTime) {

		Long ts = 0L;
		try {

			/* We expect that the 'epch' field is "0" */
			ts = Long.valueOf(unixTime.getAsString()) * 1000;

		} catch (Exception e) {
			;
		}

		return ts;
	}
}
