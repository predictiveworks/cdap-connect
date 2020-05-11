package de.kp.works.connect.redshift;
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

import java.util.List;
import com.google.common.collect.Lists;

import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.connect.jdbc.JdbcUtils;

public class RedshiftUtils extends JdbcUtils {

	private static final long serialVersionUID = 5111966838738064709L;

	public static List<String> getColumns(Schema schema, String primaryKey) throws Exception {

		List<String> columns = Lists.newArrayList();
		for (Schema.Field field : schema.getFields()) {
			
			String fieldName = field.getName();
			String fieldType = getSqlType(field.getSchema());

			Boolean isPrimaryKey = fieldName.equals(primaryKey);
			Boolean isNullable = field.getSchema().isNullable();

			String column = getColumn(fieldName, fieldType, isNullable, isPrimaryKey);
			columns.add(column);
			
		}

		return columns;

	}

	private static String getColumn(String fieldName, String fieldType, Boolean isNullable, Boolean isPrimaryKey) {
		return null;
	}
	
	private static String getSqlType(Schema schema) {
		
		String sqlType = null;
		
		Schema.Type schemaType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
	    switch (schemaType) {
	      case ARRAY:
	        break;
	      case BOOLEAN:
	        break;
	      case BYTES:
	        break;
	      case DOUBLE:
	        break;
	      case ENUM:
	        break;
	      case FLOAT:
	        break;
	      case INT:
	        break;
	      case LONG:
	        break;
	      case MAP:
	        break;
	      case NULL:
	    	  	break;
	      case RECORD:
	        break;
	      case STRING:
	        break;
	      case UNION:
	        break;
	    }
	    
		return sqlType;
	
	}
	
}
