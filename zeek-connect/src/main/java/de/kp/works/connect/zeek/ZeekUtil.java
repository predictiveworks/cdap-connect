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

	static String X_WORKS_LOG       = "x_works_log";
	static String X_WORKS_TIMESTAMP = "x_works_timestamp";

	public static Schema getSchema(JsonObject jsonObject) {

		List<Schema.Field> fields = new ArrayList<>();

		fields.add(Schema.Field.of(X_WORKS_TIMESTAMP, Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of(X_WORKS_LOG,Schema.of(Schema.Type.STRING)));

		return Schema.recordOf("ZeekLogSchema", fields);

	}

	public static StructuredRecord toRecord(byte[] event, Schema schema) throws Exception {

		String json = new String(event, StandardCharsets.UTF_8);
		return toRecord(json, schema);

	}

	public static StructuredRecord toRecord(String event, Schema schema) throws Exception {

		JsonElement jsonElement = new JsonParser().parse(event);
		if (!jsonElement.isJsonObject())
			throw new Exception(
					String.format("[%s] Zeek events must be JSON objects.", className));

		JsonObject eventObj = jsonElement.getAsJsonObject();
		JsonObject recordObj = new JsonObject();

		long timestamp = System.currentTimeMillis();
		recordObj.addProperty(X_WORKS_TIMESTAMP, timestamp);

		recordObj.addProperty(X_WORKS_LOG, eventObj.toString());

		/* Retrieve structured record */
		String json = recordObj.toString();
		return StructuredRecordStringConverter.fromJsonString(json, schema);

	}

}
