package de.kp.works.connect.iot.mqtt;
/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.connect.core.SchemaUtil;
import de.kp.works.stream.mqtt.MqttResult;

/**
 * This class contains static helper method to infer the 
 * schema of a TTN uplink message (especially the payload
 * fields are not predefined), and also transform the JSON
 * message into a schema compliant Json object
 */
public class TTNUtil implements Serializable {

	private static final long serialVersionUID = -3396512563121825093L;

	private static final List<String> COMMON_FIELDS = Arrays.asList("deviceName", "deviceType", "airtime", "latitude",
			"longitude", "altitude");

	/***** SCHEMA *****/

	public static Schema getSchema(List<MqttResult> samples) {

		List<Schema.Field> fields = new ArrayList<>();

		/*
		 * The device name is extracted from the dev_id field; this is compliant to
		 * ThingsBoard's approach
		 */
		fields.add(Schema.Field.of("deviceName", Schema.of(Schema.Type.STRING)));

		/*
		 * The device type is extracted from the app_id field; this is compliant to
		 * ThingsBoard's approach *
		 */
		fields.add(Schema.Field.of("deviceName", Schema.of(Schema.Type.STRING)));

		/* airtime of the message in nanoseconds */
		fields.add(Schema.Field.of("airtime", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

		/*
		 * Geo representation, extracted from the metadata of the TTN uplink message
		 */
		fields.add(Schema.Field.of("latitude", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
		fields.add(Schema.Field.of("longitude", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

		fields.add(Schema.Field.of("altitude", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

		/*
		 * Payload fields must be inferred from the samples
		 */
		try {
			fields.addAll(inferPayloadFields(samples));

		} catch (Exception e) {
			;
		}

		return Schema.recordOf("ttnUplinkSchema", fields);

	}

	public static List<String> getColumns(Schema schema) {
		
		List<String> fields = new ArrayList<>();
		
		for (Schema.Field field : schema.getFields()) {
			
			if (COMMON_FIELDS.contains(field.getName())) continue;
			fields.add(field.getName());
			
		}
		
		return fields;
	}

	/*
	 * We leverage the payload_fields of the provided samples to infer the maximum
	 * number of fields that are provided by these uplink messages
	 */
	private static Collection<Schema.Field> inferPayloadFields(List<MqttResult> samples) throws Exception {

		Map<String, Schema.Field> fieldMap = new HashMap<>();
		for (MqttResult sample : samples) {

			/* Parse plain byte message */
			Charset UTF8 = Charset.forName("UTF-8");

			String json = new String(sample.payload(), UTF8);
			JsonElement jsonElement = new JsonParser().parse(json);

			if (jsonElement.isJsonObject() == false)
				throw new Exception(String.format("[%s] Uplink messages are specified as JSON objects.",
						TTNUtil.class.getName()));

			JsonElement fieldsElement = jsonElement.getAsJsonObject().get("payload_fields");
			if (fieldsElement == null)
				continue;

			JsonObject fieldsObject = fieldsElement.getAsJsonObject();
			for (Map.Entry<String, JsonElement> entry : fieldsObject.entrySet()) {

				String fieldName = entry.getKey();
				JsonElement fieldValue = entry.getValue();

				if (fieldValue.isJsonArray()) {

					if (fieldMap.containsKey(fieldName))
						continue;

					/*
					 * Json Array
					 * 
					 * We accept entries that describe Arrays of primitive values
					 */
					JsonArray items = fieldValue.getAsJsonArray();
					if (items.size() == 0)
						continue;
					/*
					 * Array elements that refer to complex data types are ignored
					 */
					JsonElement item = items.get(0);
					if (item.isJsonPrimitive() == false)
						continue;

					JsonPrimitive primitive = item.getAsJsonPrimitive();
					Schema fieldSchema = SchemaUtil.primitive2Schema(primitive);

					if (fieldSchema != null)
						fieldMap.put(fieldName, Schema.Field.of(fieldName, fieldSchema));

				}

				else if (fieldValue.isJsonNull()) {

					/*
					 * Json Null
					 * 
					 * We do not append entries to the field map, if the field name does not exists.
					 * In all other cases, the occurrence of nulls is used to transform an existing
					 * schema into a nullable one
					 */
					if (fieldMap.containsKey(fieldName)) {

						Schema fieldSchema = fieldMap.get(fieldName).getSchema();
						if (fieldSchema.isNullable())
							continue;

						fieldMap.put(fieldName, Schema.Field.of(fieldName, Schema.nullableOf(fieldSchema)));
					}

					continue;

				} else if (fieldValue.isJsonObject()) {

					/*
					 * Json Object
					 * 
					 * We accept entries that describe objects of primitive entries and flatten
					 * these entries by concatinating with sub fields
					 */
					JsonObject item = fieldValue.getAsJsonObject();
					for (Map.Entry<String, JsonElement> subentry : item.entrySet()) {

						String itemName = fieldName + "_" + subentry.getKey();
						JsonElement itemValue = subentry.getValue();

						if (fieldMap.containsKey(itemName))
							continue;
						if (itemValue.isJsonPrimitive() == false)
							continue;

						JsonPrimitive primitive = itemValue.getAsJsonPrimitive();
						Schema itemSchema = SchemaUtil.primitive2Schema(primitive);

						if (itemSchema != null)
							fieldMap.put(itemName, Schema.Field.of(itemName, itemSchema));

					}

				} else {

					/* Json Primitive */

					if (fieldMap.containsKey(fieldName))
						continue;

					JsonPrimitive primitive = fieldValue.getAsJsonPrimitive();
					Schema fieldSchema = SchemaUtil.primitive2Schema(primitive);

					if (fieldSchema != null)
						fieldMap.put(fieldName, Schema.Field.of(fieldName, fieldSchema));

				}

			}

		}

		return fieldMap.values();

	}

	/***** JSON OBJECT *****/

	public static JsonObject buildJsonObject(MqttResult result, List<String> columns) throws Exception {

		/* Parse plain byte message */
		Charset UTF8 = Charset.forName("UTF-8");

		String json = new String(result.payload(), UTF8);
		JsonElement jsonElement = new JsonParser().parse(json);

		if (jsonElement.isJsonObject() == false)
			throw new Exception(
					String.format("[%s] Uplink messages are specified as JSON objects.", TTNTransform.class.getName()));

		JsonObject inObject = jsonElement.getAsJsonObject();
		JsonObject outObject = new JsonObject();

		/* deviceName */
		outObject.addProperty("deviceName", inObject.get("dev_id").getAsString());

		/* deviceType */
		outObject.addProperty("deviceType", inObject.get("app_id").getAsString());

		/* Metadata */
		JsonElement metadata = inObject.get("metadata");
		if (metadata == null)
			outObject = getMetadata(outObject, null);

		else
			outObject = getMetadata(outObject, metadata.getAsJsonObject());

		/* Payload fields */
		JsonElement payloadFields = inObject.get("payload_fields");
		if (payloadFields == null)
			outObject = getPayloadFields(outObject, null, columns);

		else
			outObject = getPayloadFields(outObject, payloadFields.getAsJsonObject(), columns);

		return outObject;

	}

	/*
	 * "metadata": { 
	 * "airtime": 46336000, 				// Airtime in nanoseconds 
	 * 
	 * "time": "1970-01-01T00:00:00Z", 	// Time when the server received the message
	 * "frequency": 868.1, 				// Frequency at which the message was sent 
	 * "modulation": "LORA", 			// Modulation that was used - LORA or FSK 
	 * "data_rate": "SF7BW125", 			// Data rate that was used - if LORA modulation 
	 * "bit_rate": 50000, 				// Bit rate that was used - if FSK modulation 
	 * "coding_rate": "4/5", 			// Coding rate that was used
	 *  
	 * "gateways": [ 
	 * { 
	 * "gtw_id": "ttn-herengracht-ams", 	// EUI of the gateway 
	 * "timestamp": 12345, 				// Timestamp when the gateway received the message 
	 * "time": "1970-01-01T00:00:00Z", 	// Time when the gateway received the message - left out when gateway does not have synchronized time 
	 * "channel": 0, 					// Channel where the gateway received the message 
	 * "rssi": -25, 						// Signal strength of the received message 
	 * "snr": 5, 						// Signal to noise ratio of the received message 
	 * "rf_chain": 0, 					// RF chain where the gateway received the message 
	 * "latitude": 52.1234, 				// Latitude of the gateway reported in its status updates 
	 * "longitude": 6.1234, 				// Longitude of the gateway 
	 * "altitude": 6 					// Altitude of the gateway 
	 * }, ...
	 * ], 
	 * 
	 * "latitude": 52.2345, // Latitude of the device 
	 * "longitude": 6.2345, // Longitude of the device 
	 * "altitude": 2 // Altitude of the device 
	 * }
	 */
	private static JsonObject getMetadata(JsonObject outObject, JsonObject metadata) {

		if (metadata == null) {

			/* airtime of the message in nanoseconds */
			outObject.add("airtime", JsonNull.INSTANCE);

			/* latitude of the device */
			outObject.add("latitude", JsonNull.INSTANCE);

			/* longitude of the device */
			outObject.add("longitude", JsonNull.INSTANCE);

			/* altitude of the device */
			outObject.add("altitude", JsonNull.INSTANCE);

		} else {

			/* airtime of the message in nanoseconds */

			Long airtime = 0L;

			JsonElement airtimeElement = metadata.get("airtime");
			if (airtimeElement != null)
				airtime = airtimeElement.getAsLong();

			outObject.addProperty("airtime", airtime);

			/* latitude of the device */

			Double latitude = 0D;

			JsonElement latitudeElement = metadata.get("latitude");
			if (latitudeElement != null)
				latitude = latitudeElement.getAsDouble();

			outObject.addProperty("latitude", latitude);

			/* longitude of the device */

			Double longitude = 0D;

			JsonElement longitudeElement = metadata.get("longitude");
			if (longitudeElement != null)
				longitude = longitudeElement.getAsDouble();

			outObject.addProperty("longitude", longitude);

			/* altitude of the device */

			Double altitude = 0D;

			JsonElement altitudeElement = metadata.get("altitude");
			if (altitudeElement != null)
				altitude = altitudeElement.getAsDouble();

			outObject.addProperty("altitude", altitude);

		}

		return outObject;

	}

	private static JsonObject getPayloadFields(JsonObject jsonObject, JsonObject fields, List<String> columns) {

		if (columns == null || columns.isEmpty())
			return jsonObject;

		for (Map.Entry<String, JsonElement> field : fields.entrySet()) {

			String fieldName = field.getKey();
			JsonElement fieldValue = field.getValue();

			if (fieldValue.isJsonArray()) {
				
				/* Json Array
				 * 
				 * Check whether this field name is part 
				 * of the columns names
				 */
				if (columns.contains(fieldName))
					jsonObject.add(fieldName, fieldValue);
				
			}

			else if (fieldValue.isJsonNull()) {
				
				/* Json Null
				 * 
				 * Check whether this field name is part 
				 * of the columns names
				 */
				if (columns.contains(fieldName))
					jsonObject.add(fieldName, fieldValue);

			} 

			else if (fieldValue.isJsonObject()) {

				/* Json Object
				 */
				JsonObject item = fieldValue.getAsJsonObject();
				for (Map.Entry<String, JsonElement> subentry : item.entrySet()) {

					String itemName = fieldName + "_" + subentry.getKey();
					JsonElement itemValue = subentry.getValue();
					
					/* 
					 * Check whether this field name is part 
					 * of the columns names
					 */
					if (columns.contains(itemName))
						jsonObject.add(itemName, itemValue);

				}

			} else {

				/* Json Primitive
				 * 
				 * Check whether this field name is part 
				 * of the columns names
				 */
				if (columns.contains(fieldName))
					jsonObject.add(fieldName, fieldValue);

			}
			
		}

		return jsonObject;

	}

}
