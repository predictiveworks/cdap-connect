package de.kp.works.connect.crate;
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

import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.connect.jdbc.JdbcUtils;

import com.google.common.collect.Lists;

import java.sql.JDBCType;
import java.util.List;

public class CrateUtils extends JdbcUtils {

	private static final long serialVersionUID = -8111877341898323808L;

	public static List<String> getColumns(Schema schema, String primaryKey) throws Exception {

		List<String> columns = Lists.newArrayList();
		for (Schema.Field field : schema.getFields()) {

			String fieldName = field.getName();
			String fieldType = getSqlType(field.getSchema());
			/*
			 * The field type can be null; in this case, the respective column is described
			 * as STRING
			 */
			Boolean isPrimaryKey = fieldName.equals(primaryKey);
			Boolean isNullable = field.getSchema().isNullable();

			String column = getColumn(fieldName, fieldType, isNullable, isPrimaryKey);
			columns.add(column);

		}

		return columns;

	}
	/*
	 * Crate DB does not support JDBC type names, e.g.
	 * BIGINT, VARCHAR etc.
	 */
	private static String getSqlType(Schema schema) {
		
		String sqlType = null;

		Schema.Type schemaType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
		switch (schemaType) {
		case ARRAY:
			Schema componentSchema = schema.getComponentSchema();
			sqlType = getArrayType(componentSchema);
			break;
		case BOOLEAN:
			sqlType = JDBCType.BOOLEAN.getName();
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
			sqlType = "LONG";
			break;
		case STRING:
			sqlType = "STRING";
			break;
		
		/** UNSUPPORTED **/
		case BYTES:
		case ENUM:
		case NULL:
		case MAP:
		case RECORD:
		case UNION:
			sqlType = "STRING";
			break;
		}
		return sqlType;

	}

	private static String getColumn(String fieldName, String fieldType, Boolean isNullable, Boolean isPrimaryKey) {

		if (isNullable)
			return String.format("%s %s", fieldName, fieldType);

		return String.format("%s %s NOT NULL", fieldName, fieldType);

	}

	private static String getArrayType(Schema schema) {

		String ftype = null;
		
		Schema.Type schemaType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();		
		switch (schemaType) {
		case ARRAY:
		case BYTES:
		case ENUM:
		case NULL:
		case MAP:
		case RECORD:
		case UNION:
			ftype = "ARRAY(STRING)";
			break;
		case BOOLEAN:
			ftype = "ARRAY(BOOLEAN)";
			break;
		case DOUBLE:
			ftype = "ARRAY(DOUBLE)";
			break;
		case FLOAT:
			ftype = "ARRAY(FLOAT)";
			break;
		case INT:
			ftype = "ARRAY(INTEGER)";
			break;
		case LONG:
			ftype = "ARRAY(LONG)";
			break;
		case STRING:
			ftype = "ARRAY(STRING)";
			break;

		}

		return ftype;

	}

	private CrateUtils() {
		throw new AssertionError("[CrateUtils] Should not instantiate static utility class.");
	}
}
