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
import java.util.List;
import java.util.stream.Collectors;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.connect.core.SchemaUtil;
import de.kp.works.stream.mqtt.MqttResult;

public class DefaultUtil implements Serializable {

	private static final long serialVersionUID = 1839935244896054913L;

	/***** SCHEMA *****/

	public static Schema getSchema(List<MqttResult> samples) {
		/*
		 * STEP #1: Transform Mqtt results into JsonObjects
		 */
		List<JsonObject> items = samples.stream().map(sample -> {
			
			/* Parse plain byte message */
			Charset UTF8 = Charset.forName("UTF-8");

			String json = new String(sample.payload(), UTF8);
			JsonElement jsonElement = new JsonParser().parse(json);

			if (jsonElement.isJsonObject() == false)
				throw new RuntimeException(String.format("[%s] Mqtt messages are specified as JSON objects.",
						DefaultUtil.class.getName()));
			
			return jsonElement.getAsJsonObject();
			
		}).collect(Collectors.toList());
		
		return SchemaUtil.inferSchema(items, 10);

	}

	/***** JSON OBJECT *****/

	public static JsonObject buildJsonObject(MqttResult result) throws Exception {
		
		/* Parse plain byte message */
		Charset UTF8 = Charset.forName("UTF-8");

		String json = new String(result.payload(), UTF8);
		JsonElement jsonElement = new JsonParser().parse(json);

		if (jsonElement.isJsonObject() == false)
			throw new RuntimeException(String.format("[%s] Mqtt messages are specified as JSON objects.",
					DefaultUtil.class.getName()));

		return jsonElement.getAsJsonObject();

	}

}
