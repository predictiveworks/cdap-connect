package de.kp.works.connect.redshift;
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

public class RedshiftUtils extends JdbcUtils {

	private static final long serialVersionUID = 5111966838738064709L;

	public static List<String> getColumns(Schema schema) {

		List<String> columns = Lists.newArrayList();

		assert schema.getFields() != null;
		for (Schema.Field field : schema.getFields()) {
			
			String fieldName = field.getName();
			String fieldType = getSqlType(field.getSchema());

			Boolean isNullable = field.getSchema().isNullable();

			String column = getColumn(fieldName, fieldType, isNullable);
			columns.add(column);
			
		}

		return columns;

	}

	private static String getColumn(String fieldName, String fieldType, Boolean isNullable) {

		if (isNullable)
			return String.format("%s %s", fieldName, fieldType);

		return String.format("%s %s NOT NULL", fieldName, fieldType);

	}
	
	private static String getSqlType(Schema schema) {
		
		String sqlType = null;
		
		Schema.Type schemaType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
	    switch (schemaType) {
	      	case BOOLEAN:
	    	  	sqlType = JDBCType.BOOLEAN.getName();
	        	break;
	      	case DOUBLE:
	    	  	sqlType = "DOUBLE PRECISION";
	       		 break;
	      	case FLOAT:
	    	  	sqlType = "REAL";
	        	break;
	      	case INT:
	    	  	sqlType = JDBCType.INTEGER.getName();
	        	break;
	      	case LONG:
	    	  	sqlType = JDBCType.BIGINT.getName();
	        	break;
	      
		  	case ARRAY:
		  	case BYTES:
		  	case ENUM:
		  	case MAP:
		  	case NULL:
		  	case RECORD:
		  	case UNION:
			case STRING:
				sqlType = JDBCType.VARCHAR.getName();
	        	break;
		}
	    
		return sqlType;
	
	}

}
