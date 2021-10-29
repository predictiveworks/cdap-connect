package de.kp.works.connect.hivemq;
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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

public class MqttTransform implements Function<String, StructuredRecord> {

	private static final long serialVersionUID = 5511944788990345893L;

	protected Schema schema;

	public MqttTransform() {
	}

	public Schema buildSchema() {

		List<Schema.Field> fields = new ArrayList<>();
		
		fields.add(Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)));
		fields.add(Schema.Field.of("seconds", Schema.of(Schema.Type.LONG)));
		
		fields.add(Schema.Field.of("topic", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("qos", Schema.of(Schema.Type.INT)));

		fields.add(Schema.Field.of("duplicate", Schema.of(Schema.Type.BOOLEAN)));
		fields.add(Schema.Field.of("retained", Schema.of(Schema.Type.BOOLEAN)));

		fields.add(Schema.Field.of("digest", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("context", Schema.of(Schema.Type.STRING)));

		fields.add(Schema.Field.of("dimension", Schema.of(Schema.Type.STRING)));
		fields.add(Schema.Field.of("payload", Schema.of(Schema.Type.STRING)));

		schema = Schema.recordOf("PahoSchema", fields);
		return schema;
		
	}

	@Override
	public StructuredRecord call(String in) {

		JsonObject json = new Gson().fromJson(in, JsonObject.class);
		Schema schema = buildSchema();

		StructuredRecord.Builder builder = StructuredRecord.builder(schema);

		builder.set("timestamp", json.get("timestamp").getAsLong());
		builder.set("seconds", json.get("seconds").getAsLong());

		builder.set("topic", json.get("topic").getAsString());
		builder.set("qos", json.get("qos").getAsInt());

		builder.set("duplicate", json.get("topic").getAsBoolean());
		builder.set("retained", json.get("qos").getAsBoolean());

		builder.set("digest", json.get("digest").getAsString());
		builder.set("context", json.get("context").getAsString());

		builder.set("dimension", json.get("dimension").getAsString());
		builder.set("payload", json.get("payload").getAsString());

		return builder.build();

	}

}
