package de.kp.works.connect;
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

/**
 * Transforms kafka key and message into a structured record when message format
 * is not given. Everything here should be serializable, as Spark Streaming will
 * serialize all functions.
 */
public class EmptyFunction implements Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> {

	private static final long serialVersionUID = -2582275414113323812L;

	@Override
	public StructuredRecord call(ConsumerRecord<byte[], byte[]> in) throws Exception {

		List<Schema.Field> schemaFields = new ArrayList<>();
		Schema schema = Schema.recordOf("emptyOutput", schemaFields);

		StructuredRecord.Builder builder = StructuredRecord.builder(schema);
		return builder.build();
		
	}

}

