package de.kp.works.connect.pubsub;
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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import de.kp.works.connect.kafka.osquery.OsqueryUtil;
import de.kp.works.stream.pubsub.PubSubResult;

public class KolideTransform extends PubSubTransform {

	private static final long serialVersionUID = 2899269164494726255L;
	private Schema schema;

	public KolideTransform() {
		
	}

	@Override
	public JavaRDD<StructuredRecord> call(JavaRDD<PubSubResult> input) throws Exception {
		
		if (input.isEmpty())
			return input.map(new EmptyPubSubTransform());

		if (schema == null) {
			schema = getSchema(input.first());
		}
		/*
		 * Schema strategy: The schema is inferred from the first record and then
		 * assigned to the event transformer;
		 * 
		 * this is a suitable strategy as the [osquery] schema is more or less static
		 * due to its strong relationship to predefined queries.
		 */
		return input.map(new ResultTransform(schema));

	}

	private Schema getSchema(PubSubResult result) throws Exception {

		String event = new String(result.data(), "UTF-8");
		JsonElement jsonElement = new JsonParser().parse(event);

		if (!jsonElement.isJsonObject())
			throw new Exception(
					String.format("[%s] Osquery events must be JSON objects.", KolideTransform.class.getName()));

		JsonObject eventObject = jsonElement.getAsJsonObject();
		return OsqueryUtil.getSchema(eventObject);

	}
	
	public class ResultTransform implements Function<PubSubResult, StructuredRecord> {

		private static final long serialVersionUID = -3784341003979482293L;

		private Schema schema;
		
		public ResultTransform(Schema schema) {
			this.schema = schema;
		}
		
		@Override
		public StructuredRecord call(PubSubResult in) throws Exception {			
			byte[] data = in.data();			
			return OsqueryUtil.toRecord(data, schema);
		}
		
	}

}
