package de.kp.works.connect.osquery;
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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class OsqueryUtil {

	static String className = OsqueryUtil.class.getName();

	static String X_WORKS_FORMAT    = "x_works_format";
	static String X_WORKS_HOSTNAME  = "x_works_hostname";
	static String X_WORKS_NAME      = "x-works_name";
	static String X_WORKS_LOG       = "x_works_log";
	static String X_WORKS_TIMESTAMP = "x_works_timestamp";

	public static Schema getSchema(JsonObject jsonObject) throws Exception {

		List<Schema.Field> fields = new ArrayList<>();

		/* name: The name of the query that generated this event. */
		if (jsonObject.get("name") == null)
			throw new Exception(
					String.format("[%s] Osquery events must contain a 'name' field.", className));

		fields.add(Schema.Field.of(X_WORKS_NAME, Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of(X_WORKS_TIMESTAMP, Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of(X_WORKS_HOSTNAME,  Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of(X_WORKS_FORMAT,    Schema.of(Schema.Type.STRING)));

		fields.add(Schema.Field.of(X_WORKS_LOG,Schema.of(Schema.Type.STRING)));
		return Schema.recordOf("OsqueryLogSchema", fields);

	}

	public static StructuredRecord toRecord(byte[] event, Schema schema) throws Exception {

		String json = new String(event, StandardCharsets.UTF_8);
		return toRecord(json, schema);

	}

	public static StructuredRecord toRecord(String event, Schema schema) throws Exception {

		JsonElement jsonElement = JsonParser.parseString(event);
		if (!jsonElement.isJsonObject())
			throw new Exception(
					String.format("[%s] Osquery events must be JSON objects.", className));

		JsonObject eventObj = jsonElement.getAsJsonObject();
		JsonObject recordObj = new JsonObject();

		recordObj.addProperty(X_WORKS_NAME, eventObj.get("name").getAsString());

		String calendarTime = eventObj.get("calendarTime").getAsString();
		recordObj.addProperty(X_WORKS_TIMESTAMP, getTimestamp(calendarTime));

		recordObj.addProperty(X_WORKS_HOSTNAME, getHostname(eventObj));

		/* Distinguish between BATCH, EVENT and SNAPSHOT logs */

		if (eventObj.has("columns")) {

			JsonObject columns = eventObj.get("columns").getAsJsonObject();

			recordObj.addProperty(X_WORKS_FORMAT, "event");
			recordObj.addProperty(X_WORKS_LOG, columns.toString());

		}
		else if (eventObj.has("diffResult")) {

			JsonArray diffResult = eventObj.get("diffResults").getAsJsonArray();

			recordObj.addProperty(X_WORKS_FORMAT, "batch");
			recordObj.addProperty(X_WORKS_LOG, diffResult.toString());

		}
		else if (eventObj.has("snapshot")) {

			JsonArray snapshot = eventObj.get("snapshot").getAsJsonArray();

			recordObj.addProperty(X_WORKS_FORMAT, "snapshot");
			recordObj.addProperty(X_WORKS_LOG, snapshot.toString());

		}
		else
			throw new Exception(
					String.format("[%s] Unknown Fleet log format detected.", className));

		/* Retrieve structured record */
		String json = recordObj.toString();
		return StructuredRecordStringConverter.fromJsonString(json, schema);

	}

	private static String getHostname(JsonObject jsonObj) {
		/*
		 * The host name is represented by different
		 * keys within the different result logs
		 */
		List<String> keys = Arrays.asList("host", "host_identifier", "hostname");
		String hostName = "";

		for (String key: keys) {
			if (jsonObj.has(key)) {
				hostName = jsonObj.get(key).getAsString();
			}
		}

		return hostName;
		
	}

	private static Long getTimestamp(String s) {

		try {

			/* Osquery use UTC to describe datetime */
			String pattern;
			if (s.endsWith("UTC")) {
				pattern = "EEE MMM dd hh:mm:ss yyyy z";
			}
			else
				pattern = "EEE MMM dd hh:mm:ss yyyy";

			SimpleDateFormat format = new SimpleDateFormat(pattern, java.util.Locale.US);
			format.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));

			Date date = format.parse(s);
			return date.getTime();

		} catch (ParseException e) {
			return 0L;
		}

	}

}
