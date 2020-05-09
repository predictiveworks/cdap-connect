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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import de.kp.works.connect.jdbc.JdbcWritable;

public class CrateWritable extends JdbcWritable {

	private static final Logger LOG = LoggerFactory.getLogger(CrateWritable.class);

	private StructuredRecord record;
	private CrateConnect connect;

	public CrateWritable(CrateConnect connect, StructuredRecord record) {
		this.connect = connect;
		this.record = record;
	}

	/**
	 * Used in map-reduce. Do not remove.
	 */
	public CrateWritable() {
	}

	@Override
	public PreparedStatement write(Connection conn, PreparedStatement stmt) throws SQLException {

		Schema schema = record.getSchema();
		List<Schema.Field> fields = schema.getFields();

		if (stmt == null) {
			try {

				List<String> columns = CrateUtils.getColumns(schema);

				if (connect.createTable(conn, columns)) {
					connect.loadColumnTypes(conn);

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

			Schema.Type fieldType = getNonNullableType(field);
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
			throw new SQLException(String.format("[SAPHanaWritable] Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
		}
	}

}
