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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import javax.annotation.Nullable;
import javax.sql.rowset.serial.SerialBlob;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

public class CrateSqlWritable implements Writable, CrateWritable, Configurable {

	private static final Logger LOG = LoggerFactory.getLogger(CrateSqlWritable.class);

	private StructuredRecord record;
	private CrateConnect connect;

	private Configuration conf;

	public CrateSqlWritable(CrateConnect connect, StructuredRecord record) {
		this.connect = connect;
		this.record = record;
	}

	/**
	 * Used in map-reduce. Do not remove.
	 */
	public CrateSqlWritable() {
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration config) {
		this.conf = config;
	}

	@Override
	public PreparedStatement write(Connection conn, PreparedStatement statement) throws SQLException {

		PreparedStatement stmt = statement;

		Schema schema = record.getSchema();
		List<Schema.Field> fields = schema.getFields();

		if (stmt == null) {
			try {

				List<String> columns = CrateUtils.getColumns(schema);

				if (connect.createTable(columns)) {
					connect.loadColumnTypes();

					List<String> fnames = Lists.newArrayList();

					for (Schema.Field field : fields) {
						fnames.add(field.getName());
					}

					String[] fieldNames = new String[fnames.size()];
					fieldNames = fnames.toArray(fieldNames);

					String insertQuery = connect.insertQuery(fieldNames);
					stmt = conn.prepareStatement(insertQuery);
				}

			} catch (Exception e) {
				LOG.error(String.format("[CrateSqlWritable] Write request failed: ", e.getLocalizedMessage()));

			}

		}

		int[] columnTypes = connect.getColumnTypes();

		for (int i = 0; i < fields.size(); i++) {

			Schema.Field field = fields.get(i);
			String fieldName = field.getName();

			Schema.Type fieldType = CrateUtils.getNonNullableType(field);
			Object fieldValue = record.get(fieldName);

			writeToCrate(stmt, fieldType, fieldValue, i, columnTypes);

		}

		return stmt;

	}

	private void writeToCrate(PreparedStatement stmt, Schema.Type fieldType, @Nullable Object fieldValue,
			int fieldIndex, int[] columnTypes) throws SQLException {

		int sqlIndex = fieldIndex + 1;
		if (fieldValue == null) {
			stmt.setNull(sqlIndex, columnTypes[fieldIndex]);
			return;
		}

		switch (fieldType) {
		case NULL:
			stmt.setNull(sqlIndex, columnTypes[fieldIndex]);
			break;
		case STRING:
			/* clob can also be written to as setString */
			stmt.setString(sqlIndex, (String) fieldValue);
			break;
		case BOOLEAN:
			stmt.setBoolean(sqlIndex, (Boolean) fieldValue);
			break;
		case INT:
			/* write short or int appropriately */
			writeInt(stmt, fieldIndex, sqlIndex, fieldValue, columnTypes);
			break;
		case LONG:
			/* write date, timestamp or long appropriately */
			writeLong(stmt, fieldIndex, sqlIndex, fieldValue, columnTypes);
			break;
		case FLOAT:
			/* both real and float are set with the same method on prepared statement */
			stmt.setFloat(sqlIndex, (Float) fieldValue);
			break;
		case DOUBLE:
			stmt.setDouble(sqlIndex, (Double) fieldValue);
			break;
		case BYTES:
			writeBytes(stmt, fieldIndex, sqlIndex, fieldValue, columnTypes);
			break;
		default:
			throw new SQLException(String.format("[CrateSqlWritable] Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
		}
	}

	private void writeBytes(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue, int[] columnTypes)
			throws SQLException {
		byte[] byteValue = (byte[]) fieldValue;
		int parameterType = columnTypes[fieldIndex];
		if (Types.BLOB == parameterType) {
			stmt.setBlob(sqlIndex, new SerialBlob(byteValue));
			return;
		}
		/* handles BINARY, VARBINARY and LOGVARBINARY */
		stmt.setBytes(sqlIndex, (byte[]) fieldValue);
	}

	private void writeInt(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue, int[] columnTypes)
			throws SQLException {

		Integer intValue = (Integer) fieldValue;
		int parameterType = columnTypes[fieldIndex];

		if (Types.TINYINT == parameterType || Types.SMALLINT == parameterType) {
			stmt.setShort(sqlIndex, intValue.shortValue());
			return;
		}

		stmt.setInt(sqlIndex, intValue);

	}

	private void writeLong(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue, int[] columnTypes)
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

	@Override
	public void readFields(DataInput input) throws IOException {
		throw new IOException("[CrateSqlWritable] Method 'readFields' from DataInput is not implemented");
	}

	@Override
	public void write(DataOutput output) throws IOException {
		throw new IOException("[CrateSqlWritable] Method 'write' from DataOutput is not implemented");
	}

}
