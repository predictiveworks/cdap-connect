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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class JdbcRecord implements Writable, DBWritable, Configurable {

	private StructuredRecord record;
	private Configuration conf;

	public JdbcRecord(Configuration config) {
		this.conf = config;
	}

	/**
	 * Used in map-reduce. Do not remove.
	 */
	public JdbcRecord() {
	}

	public void readFields(DataInput in) throws IOException {
		throw new IOException(
				String.format("[%s] Method 'write' from DataOutput is not implemented", this.getClass().getName()));
	}

	public void readFields(ResultSet resultSet) throws SQLException {

		ResultSetMetaData metadata = resultSet.getMetaData();

		List<Schema.Field> schemaFields = JdbcUtils.getSchemaFields(resultSet);
		Schema schema = Schema.recordOf("jdbcSchema", schemaFields);

		StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
		for (int i = 0; i < schemaFields.size(); i++) {

			Schema.Field field = schemaFields.get(i);

			int sqlType = metadata.getColumnType(i + 1);
			int precision = metadata.getPrecision(i + 1);

			int scale = metadata.getScale(i + 1);
			boolean signed = metadata.isSigned(i + 1);
			/*
			 * The schema fields derived by getSchemaFields ignore
			 * SQL data types of the subsequent form; for reasons of
			 * transparency, we provided the decision below (again)
			 */
			if (!ignoreField(sqlType))
				recordBuilder.set(field.getName(),
						transformValue(sqlType, precision, scale, signed, field.getName(), resultSet));

		}

		record = recordBuilder.build();

	}

	public StructuredRecord getRecord() {
		return record;
	}

	public void write(DataOutput out) throws IOException {
		throw new IOException(
				String.format("[%s] Method 'write' from DataOutput is not implemented", this.getClass().getName()));
	}

	public void write(PreparedStatement stmt) throws SQLException {
		throw new SQLException(String.format("[%s] Method 'write' from PreparedStatement is not implemented",
				this.getClass().getName()));
	}

	private boolean ignoreField(int sqlType) {

		switch (sqlType) {
		 	case Types.ARRAY: 
	 		case Types.DATALINK: 
 			case Types.DISTINCT: 
			case Types.JAVA_OBJECT: 
			case Types.OTHER: 
				return true;
				
			default:
				return false;
		
		}
		
	}
	
	private Object transformValue(int sqlType, int precision, int scale, boolean signed, String fieldName,
			ResultSet resultSet) throws SQLException {

		Object original = resultSet.getObject(fieldName);
		if (original == null)
			return original;

		switch (sqlType) {

		/* BIT & BOOLEAN */
		case Types.BOOLEAN:
		case Types.BIT:
			return resultSet.getBoolean(fieldName);

		/* BINARY | VARBINARY | LONGVARBINARY */
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return resultSet.getBytes(fieldName);

		case Types.BLOB:
			Object toReturn;
			Blob blob = resultSet.getBlob(fieldName);
			try {
				toReturn = blob.getBytes(1, (int) blob.length());
			} finally {
				blob.free();
			}
			return toReturn;

		/* BIGINT | ROWID */
		case Types.BIGINT:
		case Types.ROWID:
			return resultSet.getLong(fieldName);

		/* CHAR | VARCHAR | LONGVARCHAR */
		case Types.CHAR:
		case Types.LONGVARCHAR:
		case Types.VARCHAR:
			return resultSet.getString(fieldName);

		/* CLOB */
		case Types.CLOB: {

			Clob clob = resultSet.getClob(fieldName);

			int size = (int) clob.length();
			return clob.getSubString(1, size);

		}

		/* NCLOB */
		case Types.NCLOB: {

			NClob nclob = resultSet.getNClob(fieldName);

			int size = (int) nclob.length();
			return nclob.getSubString(1, size);

		}
		/* DATE TIME */
		case Types.DATE:
			return resultSet.getDate(fieldName).getTime();

		case Types.TIME:
			return resultSet.getTime(fieldName).getTime();

		case Types.TIMESTAMP:
			return resultSet.getTimestamp(fieldName).getTime();

		/* SMALLINT & TINYINT */
		case Types.SMALLINT:
		case Types.TINYINT:
			return new Short(resultSet.getShort(fieldName)).intValue();

		/* INTEGER */
		case Types.INTEGER: {

			if (signed)
				return resultSet.getInt(fieldName);
			else
				return resultSet.getLong(fieldName);

		}
		/* DOUBLE | DECIMAL | NUMERIC */
		case Types.DOUBLE:
			return resultSet.getDouble(fieldName);

		case Types.DECIMAL:
		case Types.NUMERIC:
			return resultSet.getBigDecimal(fieldName).doubleValue();

		/* FLOAT */
		case Types.FLOAT:
		case Types.REAL:
			return resultSet.getFloat(fieldName);

		/* LONGNVARCHAR | NCHAR | NVARCHAR */
		case Types.LONGNVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
			return resultSet.getNString(fieldName);

		/* STRING REPRESENTATION */
		case Types.REF:
			return resultSet.getRef(fieldName).toString();

		case Types.SQLXML:
			return resultSet.getSQLXML(fieldName).toString();

		case Types.STRUCT:
			return (String) original;
		}
		
		return original;
		
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
}