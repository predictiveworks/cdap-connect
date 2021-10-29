package de.kp.works.connect.osquery.pubsub.transform;
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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import de.kp.works.stream.pubsub.*;

public abstract class PubSubTransform implements Function<JavaRDD<PubSubResult>, JavaRDD<StructuredRecord>> {
	
	private static final long serialVersionUID = 2354453038164761737L;

	/**
	 * This method transforms an empty stream batch into
	 * a dummy structured record
	 */
	public static class EmptyPubSubTransform implements Function<PubSubResult, StructuredRecord> {

		private static final long serialVersionUID = 6345145292791935039L;

		@Override
		public StructuredRecord call(PubSubResult in) {

			List<Schema.Field> schemaFields = new ArrayList<>();
			Schema schema = Schema.recordOf("pubSubSchema", schemaFields);

			StructuredRecord.Builder builder = StructuredRecord.builder(schema);
			return builder.build();
			
		}

	}

}
