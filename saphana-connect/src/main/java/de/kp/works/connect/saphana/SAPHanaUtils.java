package de.kp.works.connect.saphana;
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

import java.sql.JDBCType;
import java.util.List;

import com.google.common.collect.Lists;

import de.kp.works.connect.common.jdbc.JdbcUtils;
import io.cdap.cdap.api.data.schema.Schema;

public class SAPHanaUtils extends JdbcUtils {
	
	private static final long serialVersionUID = -6304203493395042649L;
	
	private static final Character ESCAPE_CHAR = '"';

	public static List<String> getColumns(Schema schema, String primaryKey) {

		List<String> columns = Lists.newArrayList();

		assert schema.getFields() != null;
		for (Schema.Field field : schema.getFields()) {
			
			String fieldName = field.getName();
			String fieldType = getSqlType(field.getSchema());

			Boolean isPrimaryKey = fieldName.equals(primaryKey);

			String column = getColumn(fieldName, fieldType, isPrimaryKey);
			columns.add(column);
			
		}

		return columns;
	}

	private static String getColumn(String fieldName, String fieldType, Boolean isPrimaryKey) {
		
		if (isPrimaryKey) {
			/*
			 * Other than in UPSERT statements, SAP Hana expects
			 * escaped field names in CREATE ROW TABLE statements
			 */
			fieldName = ESCAPE_CHAR + fieldName + ESCAPE_CHAR;
			return String.format("%s %s PRIMARY KEY", fieldName, fieldType);
			
		} else {
			
			fieldName = ESCAPE_CHAR + fieldName + ESCAPE_CHAR;
			return String.format("%s %s", fieldName, fieldType);

		}
		
	}

	private static String getSqlType(Schema schema) {
		
		String sqlType = null;
		
		Schema.Type schemaType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
	    switch (schemaType) {
	  		case ARRAY:
				sqlType = JDBCType.ARRAY.getName();
				break;
		  	case BOOLEAN:
		  		sqlType = JDBCType.BOOLEAN.getName();
			  	break;
		  	case BYTES:
		  		sqlType = JDBCType.BINARY.getName();
	        	break;
	      	case DOUBLE:
			  	sqlType = JDBCType.DOUBLE.getName();
			  	break;
	      	case ENUM:
			case MAP:
			case RECORD:
			case STRING:
			case UNION:
				sqlType = JDBCType.VARCHAR.getName();
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
			case NULL:
	      		sqlType = JDBCType.NULL.getName();
	    	  	break;
		}
	    
		return sqlType;
	
	}
}
