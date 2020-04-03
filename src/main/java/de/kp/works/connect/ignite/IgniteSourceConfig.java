package de.kp.works.connect.ignite;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

public class IgniteSourceConfig extends IgniteConfig {

	private static final long serialVersionUID = -2818369977710528068L;

	@Description("The comma-separated list of field names that are used to extract from the specified Ignite cache.")
	@Macro
	public String fieldNames;

	@Description("The number of partitions to organize the data of the specified Ignite cache. Default is 1.")
	@Macro
	public int partitions;
	
	public IgniteSourceConfig() {
		super();
		
		partitions = 1;

	}
	
	@Override
	public Properties getConfig() {

		Properties config = super.getConfig();
		
		config.setProperty(IgniteUtil.IGNITE_FIELDS, fieldNames);		
		config.setProperty(IgniteUtil.IGNITE_PARTITIONS, String.valueOf(partitions));
		
		return config;
		
	}
	
	public StructuredRecord values2Record(List<Object> values, Schema schema) {

		Map<String, Integer> indices = getIndices(fieldNames);
		
		StructuredRecord.Builder builder = StructuredRecord.builder(schema);
		for (Schema.Field field : schema.getFields()) {

			String fieldName = field.getName();
			Integer fieldIndex = indices.get(fieldName);
			/*
			 * The list of values contains the _key field
			 * as its initial or first value; we therefore
			 * have to increment by one to get user field
			 * values
			 */			
			Object fieldValue = values.get(fieldIndex + 1);
			/*
			 * We do not expect nullable field schemas;
			 * therefore no addition check is performed
			 */
			Schema fieldSchema = field.getSchema();
			/*
			 * Apache Ignite supports date & time data
			 * types which are all mapped onto LONG 
			 */
			if (fieldSchema.getType().equals(Schema.Type.LONG))
				fieldValue = datetime2Long(fieldValue);
			
			builder.set(fieldName, fromFieldValue(fieldValue, fieldSchema));

		}

		return builder.build();

	}

	private Map<String, Integer> getIndices(String value) {

		Map<String, Integer> indices = new HashMap<>();
		
		String[] tokens = IgniteUtil.string2Array(value);
		for (int i= 0; i < tokens.length; i++) {
			indices.put(tokens[i], i);
		}
		
		return indices;
		
	}
	
	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(fieldNames)) {
			throw new IllegalArgumentException(
					String.format("[%s] The cache field names must not be empty.", this.getClass().getName()));
		}

		if (partitions < 1) {
			throw new IllegalArgumentException(
					String.format("[%s] The number of partitions must be positive.", this.getClass().getName()));
		}
		
	}

}
