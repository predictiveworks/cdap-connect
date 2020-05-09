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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

import com.google.common.collect.Lists;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;

public class JdbcUtils implements Serializable {

	private static final long serialVersionUID = 4941026070769773563L;

	public static boolean createTable(Connection conn, String tableName, String createSql) {

		Statement stmt = null;
		Boolean success = false;

		try {

			if (tableExists(conn, tableName) == false) {

				conn.setAutoCommit(false);

				stmt = conn.createStatement();
				stmt.execute(createSql);

				conn.commit();
				stmt.close();

				success = true;
			
			} else
				success = true;

		} catch (SQLException e) {
			;
			
		} finally {

			if (stmt != null)
				try {
					stmt.close();

				} catch (SQLException e) {
					;
				}

		}

		return success;

	}

	public static Boolean tableExists(Connection conn, String table) throws SQLException {

		DatabaseMetaData metadata = conn.getMetaData();

		ResultSet rs = metadata.getTables(null, null, table, null);
		return (rs.next());

	}

	public static List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {

		List<Schema.Field> schemaFields = Lists.newArrayList();
		ResultSetMetaData metadata = resultSet.getMetaData();

		for (int i = 1; i <= metadata.getColumnCount(); i++) {

			String columnName = metadata.getColumnName(i);
			int columnSqlType = metadata.getColumnType(i);

			Schema columnSchema = Schema.of(getSchemaType(columnSqlType));

			if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
				columnSchema = Schema.nullableOf(columnSchema);
			}

			Schema.Field field = Schema.Field.of(columnName, columnSchema);
			schemaFields.add(field);

		}

		return schemaFields;

	}

	public static Schema.Type getSchemaType(int sqlType) throws SQLException {
		/*
		 * Type.STRING covers the following SQL types:
		 * 
		 * VARCHAR, CHAR, CLOB, LONGNVARCHAR, LONGVARCHAR, NCHAR, NCLOB, NVARCHAR
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
			throw new SQLException(new UnsupportedTypeException(
					String.format("[%s] Unsupported SQL Type: %s", JdbcUtils.class.getName(), sqlType)));
		}

		return type;
	}

}
