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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import javax.sql.rowset.serial.SerialBlob;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public abstract class JdbcWritable implements Configurable, Writable {

	public abstract PreparedStatement write(Connection conn, PreparedStatement stmt) throws SQLException;

	private Configuration conf;

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

	protected void writeBytes(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue,
			int[] columnTypes) throws SQLException {

		byte[] byteValue = (byte[]) fieldValue;
		int parameterType = columnTypes[fieldIndex];

		if (Types.BLOB == parameterType) {
			stmt.setBlob(sqlIndex, new SerialBlob(byteValue));
			return;
		}

		/* handles BINARY, VARBINARY and LOGVARBINARY */
		stmt.setBytes(sqlIndex, (byte[]) fieldValue);

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
