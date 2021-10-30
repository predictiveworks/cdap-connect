package de.kp.works.connect.zeek;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ZeekUtil {

	static String className = ZeekUtil.class.getName();

	static String X_WORKS_FORMAT    = "x_works_format";
	static String X_WORKS_ORIGIN    = "x_works_origin";
	static String X_WORKS_LOG       = "x_works_log";
	static String X_WORKS_TIMESTAMP = "x_works_timestamp";

	public static Schema getSchema(JsonObject jsonObject) {

		List<Schema.Field> fields = new ArrayList<>();

		fields.add(Schema.Field.of(X_WORKS_TIMESTAMP, Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of(X_WORKS_ORIGIN, Schema.of(Schema.Type.STRING)));

		fields.add(Schema.Field.of(X_WORKS_FORMAT,Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of(X_WORKS_LOG,Schema.of(Schema.Type.STRING)));

		return Schema.recordOf("ZeekSchema", fields);

	}
	/*
	 * Kafka based and stream based Zeek log events have different
	 * format and are handled with different schemas
	 */
	public static StructuredRecord toRecord(byte[] event, String origin, Schema schema) throws Exception {

		String json = new String(event, StandardCharsets.UTF_8);
		return toRecord(json, origin, schema);

	}

	public static StructuredRecord toRecord(String event, String origin, Schema schema) throws Exception {

		JsonElement jsonElement = JsonParser.parseString(event);
		if (!jsonElement.isJsonObject())
			throw new Exception(
					String.format("[%s] Zeek events must be JSON objects.", className));

		JsonObject eventObj = jsonElement.getAsJsonObject();
		JsonObject recordObj = new JsonObject();

		long timestamp = System.currentTimeMillis();
		recordObj.addProperty(X_WORKS_TIMESTAMP, timestamp);

		recordObj.addProperty(X_WORKS_ORIGIN, origin);
		if (origin.equals("kafka")) {
			/*
			 * The event format is not provided with Zeek log events
			 * that are published via Kafka
			 */
			recordObj.addProperty(X_WORKS_FORMAT, "");
			recordObj.addProperty(X_WORKS_LOG, eventObj.toString());

		}
		else if (origin.equals("stream")) {
			/*
			 * Works Stream leverages an SSE-like event format and
			 * provides the log file name (e.g. http.log) as event
			 * type.
			 */
			String format = eventObj.get("type").getAsString();
			recordObj.addProperty(X_WORKS_FORMAT, format);

			String data = eventObj.get("data").getAsString();
			recordObj.addProperty(X_WORKS_LOG, data);

		}
		else {
			throw new Exception(
					String.format("[%s] Zeek events have an unknown origin.", className));

		}

		/* Retrieve structured record */
		String json = recordObj.toString();
		return StructuredRecordStringConverter.fromJsonString(json, schema);

	}

}
