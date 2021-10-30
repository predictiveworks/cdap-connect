package de.kp.works.connect.zeek.stream.transform;
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

import de.kp.works.connect.zeek.ZeekUtil;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.spark.api.java.function.Function;

public class LogTransform implements Function<String, StructuredRecord> {

	private static final long serialVersionUID = 4014403389657429566L;

	private final Schema schema;

	public LogTransform(Schema schema) {
		this.schema = schema;
	}
	
	@Override
	public StructuredRecord call(String input) throws Exception {
		String origin = "stream";
		return ZeekUtil.toRecord(input, origin, schema);
	}

}
