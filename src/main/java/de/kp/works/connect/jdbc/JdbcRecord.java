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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import javax.annotation.Nullable;

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

			int sqlColumnType = metadata.getColumnType(i + 1);
			recordBuilder.set(field.getName(), transformValue(sqlColumnType, resultSet.getObject(field.getName())));

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

	@Nullable
	private Object transformValue(int sqlColumnType, Object original) throws SQLException {

		if (original != null) {

			switch (sqlColumnType) {
			case Types.SMALLINT:
			case Types.TINYINT:
				return ((Number) original).intValue();
			case Types.NUMERIC:
			case Types.DECIMAL:
				return ((BigDecimal) original).doubleValue();
			case Types.DATE:
				return ((Date) original).getTime();
			case Types.TIME:
				return ((Time) original).getTime();
			case Types.TIMESTAMP:
				return ((Timestamp) original).getTime();
			case Types.BLOB:
				Object toReturn;
				Blob blob = (Blob) original;
				try {
					toReturn = blob.getBytes(1, (int) blob.length());
				} finally {
					blob.free();
				}
				return toReturn;
			case Types.CLOB:
				String s;
				StringBuilder sbf = new StringBuilder();
				Clob clob = (Clob) original;
				try {
					try (BufferedReader br = new BufferedReader(clob.getCharacterStream(1, (int) clob.length()))) {
						if ((s = br.readLine()) != null) {
							sbf.append(s);
						}
						while ((s = br.readLine()) != null) {
							sbf.append(System.getProperty("line.separator"));
							sbf.append(s);
						}
					}
				} catch (IOException e) {
					throw new SQLException(e);
				} finally {
					clob.free();
				}
				return sbf.toString();
			}
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