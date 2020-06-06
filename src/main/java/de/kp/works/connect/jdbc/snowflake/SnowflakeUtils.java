package de.kp.works.connect.jdbc.snowflake;
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

import java.sql.JDBCType;
import java.util.List;

import com.google.common.collect.Lists;

import io.cdap.cdap.api.data.schema.Schema;
import de.kp.works.connect.jdbc.JdbcUtils;

public class SnowflakeUtils extends JdbcUtils {

	private static final long serialVersionUID = -7220904714465273495L;

	public static List<String> getColumns(Schema schema, String primaryKey) throws Exception {

		List<String> columns = Lists.newArrayList();
		for (Schema.Field field : schema.getFields()) {
			
			String fieldName = field.getName();
			String fieldType = getSqlType(field.getSchema());
			/*
			 * The field type can be null; in this case, the respective
			 * column is described as STRING
			 */
			Boolean isPrimaryKey = fieldName.equals(primaryKey);
			Boolean isNullable = field.getSchema().isNullable();

			String column = getColumn(fieldName, fieldType, isNullable, isPrimaryKey);
			columns.add(column);
			
		}

		return columns;

	}

	private static String getColumn(String fieldName, String fieldType, Boolean isNullable, Boolean isPrimaryKey) {
		
		if (isNullable)
			return String.format("%s %s", fieldName, fieldType);
		
		return String.format("%s %s NOT NULL", fieldName, fieldType);
		
	}

	private static String getSqlType(Schema schema) {
		
		String sqlType = null;
		
		Schema.Type schemaType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
	    switch (schemaType) {
	      case ARRAY:
	    	  	/* Snowflake specific */
	    	  	sqlType = "ARRAY";
	      case BOOLEAN:
	    	  	sqlType = JDBCType.BOOLEAN.getName(); 
	        break;
	      case BYTES:
	    	  	/* Snowflake specific */
	    	  	sqlType = "BINARY";
	        break;
	      case DOUBLE:
	    	  	sqlType = JDBCType.DOUBLE.getName();
	        break;
	      case FLOAT:
	    	  	sqlType = JDBCType.FLOAT.getName();
	        break;
	      case INT:
	    	  	sqlType = JDBCType.INTEGER.getName();
	        break;
	      case LONG:
	    	  	sqlType = JDBCType.BIGINT.getName();
	        break;
	      case STRING:
	    	  	sqlType = JDBCType.VARCHAR.getName();
	        break;
	       
	      /** UNSUPPORTED **/
	      case ENUM:
	      case NULL:
	      case MAP:
	      case RECORD:
	      case UNION:
	    	  	sqlType = JDBCType.VARCHAR.getName();
	        break;
	    }
	    
		return sqlType;
	
	}		

}
