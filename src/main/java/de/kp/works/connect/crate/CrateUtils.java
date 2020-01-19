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
import co.cask.cdap.api.data.schema.UnsupportedTypeException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrateUtils {

	private static final Logger LOG = LoggerFactory.getLogger(CrateUtils.class);

	public static List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {

		List<Schema.Field> schemaFields = Lists.newArrayList();
		ResultSetMetaData metadata = resultSet.getMetaData();

		for (int i = 1; i <= metadata.getColumnCount(); i++) {

			String columnName = metadata.getColumnName(i);
			int columnSqlType = metadata.getColumnType(i);

			Schema columnSchema = Schema.of(getType(columnSqlType));

			if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
				columnSchema = Schema.nullableOf(columnSchema);
			}

			Schema.Field field = Schema.Field.of(columnName, columnSchema);
			schemaFields.add(field);

		}

		return schemaFields;

	}

	public static Schema.Type getNonNullableType(Schema.Field field) {

		Schema.Type type;
		if (field.getSchema().isNullable()) {
			type = field.getSchema().getNonNullable().getType();

		} else {
			type = field.getSchema().getType();

		}

		Preconditions.checkArgument(type.isSimpleType(),
				"[CrateUtils] Only simple types are supported (boolean, int, long, float, double, string, bytes) "
						+ "for writing a database record, but found '%s' as the type for column '%s'. Please "
						+ "remove this column or transform it to a simple type.",
				type, field.getName());
		return type;

	}
	
	public static List<String> getColumns(Schema schema) throws Exception {
		
		List<String> columns = Lists.newArrayList();
		for (Schema.Field field : schema.getFields()) {
			
			String fname = field.getName();
			String ftype = null;
			
			Schema fschema = field.getSchema();
			
			switch (fschema.getType()) {
			case ARRAY:
		        Schema componentSchema = schema.getComponentSchema();
		        ftype = getArrayType(componentSchema);	
		        break;
			case BOOLEAN:
				ftype = "boolean";				
		        break;
			case BYTES:
				throw new Exception("[CrateUtils] BYTES is not supported");				
			case DOUBLE:
				ftype = "double";				
		        break;
			case ENUM:
				ftype = "string";
		        break;
			case FLOAT:
				ftype = "float";				
		        break;
			case INT:
				ftype = "integer";				
		        break;
			case LONG:
				ftype = "long";				
		        break;
			case NULL:
				throw new Exception("[CrateUtils] NULL is not supported");		
			case MAP:
				throw new Exception("[CrateUtils] MAP is not supported");		
			case RECORD:
				throw new Exception("[CrateUtils] RECORD is not supported");						
			case STRING:
				ftype = "string";
		        break;
			case UNION:
				throw new Exception("[CrateUtils] UNION is not supported");										
			}
			
			String column = String.format("%s %s", fname, ftype);
			columns.add(column);

		}
		
		return columns;
		
	}
	
	private static String getArrayType(Schema schema) throws Exception {
		
		switch (schema.getType()) {
		case ARRAY:
			throw new Exception("[CrateUtils] ARRAY is not supported");				
		case BOOLEAN:
			return "array(boolean)";				
		case BYTES:
			throw new Exception("[CrateUtils] BYTES is not supported");				
		case DOUBLE:
			return "array(double)";				
		case ENUM:
			return "array(string)";
		case FLOAT:
			return "array(float)";				
		case INT:
			return "array(integer)";				
		case LONG:
			return "array(long)";				
		case NULL:
			throw new Exception("[CrateUtils] NULL is not supported");		
		case MAP:
			throw new Exception("[CrateUtils] MAP is not supported");		
		case RECORD:
			throw new Exception("[CrateUtils] RECORD is not supported");						
		case STRING:
			return "array(string)";
		case UNION:
			throw new Exception("[CrateUtils] UNION is not supported");			

		}
		
		return null;
		
	}	
	
	private static Schema.Type getType(int sqlType) throws SQLException {
		/* 
		 * Type.STRING covers the following SQL types:
		 * 
		 *  VARCHAR,
		 *  CHAR,
		 *  CLOB,
		 *  LONGNVARCHAR,
		 *  LONGVARCHAR,
		 *  NCHAR,
		 *  NCLOB,
		 *  NVARCHAR 
		 */
		Schema.Type type = Schema.Type.STRING;
		switch (sqlType) {
		case Types.NULL:
			type = Schema.Type.NULL;
			break;

		case Types.BOOLEAN:
		case Types.BIT:
			type = Schema.Type.BOOLEAN;
			break;

		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			type = Schema.Type.INT;
			break;

		case Types.BIGINT:
			type = Schema.Type.LONG;
			break;

		case Types.REAL:
		case Types.FLOAT:
			type = Schema.Type.FLOAT;
			break;

		case Types.NUMERIC:
		case Types.DECIMAL:
		case Types.DOUBLE:
			type = Schema.Type.DOUBLE;
			break;

		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			type = Schema.Type.LONG;
			break;

		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		case Types.BLOB:
			type = Schema.Type.BYTES;
			break;

		case Types.ARRAY:
		case Types.DATALINK:
		case Types.DISTINCT:
		case Types.JAVA_OBJECT:
		case Types.OTHER:
		case Types.REF:
		case Types.ROWID:
		case Types.SQLXML:
		case Types.STRUCT:
			throw new SQLException(new UnsupportedTypeException("[CrateUtils] Unsupported SQL Type: " + sqlType));
		}

		return type;
	}

	/**
	 * De-register all SQL drivers that are associated with the class
	 */
	public static void deregisterAllDrivers(Class<? extends Driver> driverClass)
			throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {

		java.lang.reflect.Field field = DriverManager.class.getDeclaredField("registeredDrivers");
		field.setAccessible(true);
		
		List<?> list = (List<?>) field.get(null);
		for (Object driverInfo : list) {
			Class<?> driverInfoClass = CrateUtils.class.getClassLoader().loadClass("java.sql.DriverInfo");
			java.lang.reflect.Field driverField = driverInfoClass.getDeclaredField("driver");
			driverField.setAccessible(true);
			Driver d = (Driver) driverField.get(driverInfo);
			if (d == null) {
				LOG.debug("[CrateUtils] Found null driver object in drivers list. Ignoring.");
				continue;
			}
			LOG.debug("Removing non-null driver object from drivers list.");
			ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
			if (registeredDriverClassLoader == null) {
				LOG.debug(
						"[CrateUtils] Found null classloader for default driver {}. Ignoring since this may be using system classloader.",
						d.getClass().getName());
				continue;
			}
			// Remove all objects in this list that were created using the classloader of
			// the caller.
			if (d.getClass().getClassLoader().equals(driverClass.getClassLoader())) {
				LOG.debug("[CrateUtils] Removing default driver {} from registeredDrivers", d.getClass().getName());
				list.remove(driverInfo);
			}
		}
	}

	private CrateUtils() {
		throw new AssertionError("[CrateUtils] Should not instantiate static utility class.");
	}
}
