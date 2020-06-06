package de.kp.works.connect.jdbc;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;
import javax.sql.rowset.serial.SerialBlob;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.google.gson.Gson;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

public abstract class JdbcWritable implements Configurable, Writable {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcWritable.class);

	protected Configuration conf;

	protected JdbcConnect connect;
	protected StructuredRecord record;

	private Gson GSON = new Gson();
	
	public PreparedStatement write(Connection conn, PreparedStatement stmt) throws SQLException {

		Schema schema = record.getSchema();
		List<Schema.Field> fields = schema.getFields();

		if (stmt == null) {
			try {

				List<String> columns = getColumns(schema);

				if (connect.createTable(conn, columns)) {
					connect.loadColumnTypes(conn);

					List<String> fnames = Lists.newArrayList();

					for (Schema.Field field : fields) {
						fnames.add(field.getName());
					}

					String[] fieldNames = new String[fnames.size()];
					fieldNames = fnames.toArray(fieldNames);

					String writeQuery = connect.writeQuery(fieldNames);
					stmt = conn.prepareStatement(writeQuery);

				} else
					throw new Exception("Provided table cannot be created.");

			} catch (Exception e) {
				LOG.error(String.format("[CrateSqlWritable] Write request failed: ", e.getLocalizedMessage()));

			}

		}

		int[] columnTypes = connect.getColumnTypes();

		for (int i = 0; i < fields.size(); i++) {

			Schema.Field field = fields.get(i);
			
			String fieldName = field.getName();
			Schema fieldSchema = field.getSchema();

			Object fieldValue = record.get(fieldName);
			writeToDB(conn, stmt, fieldSchema, fieldValue, i, columnTypes);

		}

		return stmt;

	}

	/*
	 * Retrieve columns from provided schema to build write (insert or upsert
	 * query).
	 */
	public abstract List<String> getColumns(Schema schema) throws Exception;

	@Override
	public void readFields(DataInput input) throws IOException {
		throw new IOException(String.format("[%s] Method 'readFields' from DataInput is not implemented",
				JdbcWritable.class.getName()));
	}

	@Override
	public void write(DataOutput output) throws IOException {
		throw new IOException(
				String.format("[%s] Method 'write' from DataOutput is not implemented", JdbcWritable.class.getName()));
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration config) {
		this.conf = config;
	}

	public static Schema.Type getNonNullableType(Schema schema) {

		Schema.Type type;
		if (schema.isNullable()) {
			type = schema.getNonNullable().getType();

		} else {
			type = schema.getType();

		}

		return type;

	}

	/*
	 * Usually, the table does not exist and the connect.createQuery method is used
	 * to define and create a new table; in this case, all unsupported CDAP data
	 * types (e.g. array, map, record etc) are mapped into STRING
	 */
	protected void writeToDB(Connection conn, PreparedStatement stmt, Schema fieldSchema, @Nullable Object fieldValue, int fieldIndex,
			int[] columnTypes) throws SQLException {

		int sqlIndex = fieldIndex + 1;
		if (fieldValue == null) {
			stmt.setNull(sqlIndex, columnTypes[fieldIndex]);
			return;
		}
		
		Schema.Type fieldType = getNonNullableType(fieldSchema);

		switch (fieldType) {
		case ARRAY: {
			writeArray(conn, stmt, fieldIndex, sqlIndex, fieldValue, fieldSchema, columnTypes);
			break;
		}
		case BOOLEAN:
			stmt.setBoolean(sqlIndex, (Boolean) fieldValue);
			break;
		case BYTES:
			writeBytes(stmt, fieldIndex, sqlIndex, fieldValue, columnTypes);
			break;
		case DOUBLE:
			stmt.setDouble(sqlIndex, (Double) fieldValue);
			break;
		case ENUM: {
			/*
			 * ENUM data types are serialized and represented
			 * as STRING values
			 */
			writeJson(stmt, sqlIndex, fieldValue);
			break;
		}
		case FLOAT:
			stmt.setFloat(sqlIndex, (Float) fieldValue);
			break;
		case INT:
			/* write short or int appropriately */
			writeInt(stmt, fieldIndex, sqlIndex, fieldValue, columnTypes);
			break;
		case LONG:
			/* write date, timestamp or long appropriately */
			writeLong(stmt, fieldIndex, sqlIndex, fieldValue, columnTypes);
			break;
		case MAP: {
			/*
			 * MAP data types are serialized and represented
			 * as STRING values
			 */
			writeJson(stmt, sqlIndex, fieldValue);
			break;
		}
		case NULL: {
			/*
			 * NULL data types are serialized and represented
			 * as STRING values
			 */
			writeJson(stmt, sqlIndex, fieldValue);
			break;
		}
		case RECORD:
			/*
			 * RECORD data types are serialized and represented
			 * as STRING values
			 */
			writeJson(stmt, sqlIndex, fieldValue);
			break;
		case STRING:
			stmt.setString(sqlIndex, (String) fieldValue);
			break;
		case UNION:
			throw new SQLException("Data type UNION is not supported");
		}
	}
	
	protected java.sql.Array getSqlArray(Connection conn, Schema schema, List<Object> values) throws SQLException {
				
		Schema.Type schemaType = getNonNullableType(schema);
		switch(schemaType) {
		case ARRAY:
		case BYTES:
		case MAP:
		case NULL:		
		case RECORD:	 {		
			
			/* These nested data types are serialized as String */
			String typeName = JDBCType.VARCHAR.getName();
			String[] elements = new String[values.size()];
			
			for (int i=0; i < values.size(); i++) {
				elements[i] = GSON.toJson(values.get(i));
			}
			
			return conn.createArrayOf(typeName, elements);

		}
		/* Basic data type */
		case BOOLEAN: {
			
			String typeName = JDBCType.BOOLEAN.getName();
			Boolean[] elements = new Boolean[values.size()];
			
			for (int i=0; i < values.size(); i++) {
				elements[i] = (Boolean)values.get(i);
			}
			
			return conn.createArrayOf(typeName, elements);

		}
		case DOUBLE: {
			
			String typeName = JDBCType.DOUBLE.getName();
			Double[] elements = new Double[values.size()];
			
			for (int i=0; i < values.size(); i++) {
				elements[i] = (Double)values.get(i);
			}
			
			return conn.createArrayOf(typeName, elements);

		}
		case ENUM: {
			/*
			 * ENUM data types are serialized and represented
			 * as STRING values
			 */
			
			String typeName = JDBCType.VARCHAR.getName();
			String[] elements = new String[values.size()];
			
			for (int i=0; i < values.size(); i++) {
				elements[i] = GSON.toJson(values.get(i));
			}
			
			return conn.createArrayOf(typeName, elements);

		}
		case FLOAT: {
			
			String typeName = JDBCType.FLOAT.getName();
			Float[] elements = new Float[values.size()];
			
			for (int i=0; i < values.size(); i++) {
				elements[i] = (Float)values.get(i);
			}
			
			return conn.createArrayOf(typeName, elements);

		}
		case INT: {
			
			String typeName = JDBCType.INTEGER.getName();
			Integer[] elements = new Integer[values.size()];
			
			for (int i=0; i < values.size(); i++) {
				elements[i] = (Integer)values.get(i);
			}
			
			return conn.createArrayOf(typeName, elements);

		}
		case LONG: {
			
			String typeName = JDBCType.BIGINT.getName();
			Long[] elements = new Long[values.size()];
			
			for (int i=0; i < values.size(); i++) {
				elements[i] = (Long)values.get(i);
			}
			
			return conn.createArrayOf(typeName, elements);

		}
		case STRING: {
			
			String typeName = JDBCType.VARCHAR.getName();
			String[] elements = new String[values.size()];
			
			for (int i=0; i < values.size(); i++) {
				elements[i] = (String)values.get(i);
			}
			
			return conn.createArrayOf(typeName, elements);

		}
		case UNION:
			throw new SQLException("Data type UNION is not supported");
		}
		
		return null;
	}

	/*
	 * Databases such as CRATE DB exist, that support ARRAY 
	 * data types
	 */
	protected void writeArray(Connection conn, PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue,
			Schema schema, int[] columnTypes) throws SQLException {
		
		int parameterType = columnTypes[fieldIndex];
		switch(parameterType) {
		case Types.ARRAY: {
			/*
			 * This use case e.g. is supported by Crate DB; currently, 
			 * we do not support nested arrays, i.e. the component
			 * schema must be a basic data type 
			 */
	        List<Object> values = new ArrayList<>((Collection<?>) fieldValue);
	        java.sql.Array arrayValues = getSqlArray(conn, schema.getComponentSchema(), values);
	        
	        stmt.setArray(sqlIndex, arrayValues);
			break;
		}
		default:
			String json = GSON.toJson(fieldValue);
			stmt.setString(sqlIndex, json);

		}
		
	}

	protected void writeJson(PreparedStatement stmt, int sqlIndex, Object fieldValue) throws SQLException {

		String json = GSON.toJson(fieldValue);
		stmt.setString(sqlIndex, json);
		
	}
	/*
	 * Databases such as the CRATE DB exist, that do not
	 * support BYTES data type; unsupported data types are
	 * commonly mapped into STRING
	 */
	protected void writeBytes(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue,
			int[] columnTypes) throws SQLException {

		int parameterType = columnTypes[fieldIndex];
		switch(parameterType) {
		/* BLOB */
		case Types.BLOB: {
			
			byte[] byteValue = (byte[]) fieldValue;
			stmt.setBlob(sqlIndex, new SerialBlob(byteValue));
			break;
		}
		/* STRING */
		case Types.CHAR:
		case Types.LONGVARCHAR:
		case Types.VARCHAR: {

			String json = GSON.toJson(fieldValue);
			stmt.setString(sqlIndex, json);

			break;
			
		}
		default:
			/* handles BINARY, VARBINARY and LOGVARBINARY */
			stmt.setBytes(sqlIndex, (byte[]) fieldValue);
			
		}


	}

	protected void writeInt(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue, int[] columnTypes)
			throws SQLException {

		Integer intValue = (Integer) fieldValue;
		int parameterType = columnTypes[fieldIndex];

		if (Types.TINYINT == parameterType || Types.SMALLINT == parameterType) {
			stmt.setShort(sqlIndex, intValue.shortValue());
			return;
		}

		stmt.setInt(sqlIndex, intValue);

	}

	/*
	 * CDAP does not support date, time or timestamp but leverages [Long] to
	 * represent times. This method transforms [Long] into the output format of the
	 * associated Jdbc table
	 */
	protected void writeLong(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue, int[] columnTypes)
			throws SQLException {

		Long longValue = (Long) fieldValue;

		switch (columnTypes[fieldIndex]) {
		case Types.DATE:
			stmt.setDate(sqlIndex, new Date(longValue));
			break;
		case Types.TIME:
			stmt.setTime(sqlIndex, new Time(longValue));
			break;
		case Types.TIMESTAMP:
			stmt.setTimestamp(sqlIndex, new Timestamp(longValue));
			break;
		default:
			stmt.setLong(sqlIndex, longValue);
			break;
		}

	}

}
