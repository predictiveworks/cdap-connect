package de.kp.works.connect.common.jdbc;
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

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;

import java.io.Serializable;
import java.sql.*;
import java.util.List;

public class JdbcUtils implements Serializable {

	private static final long serialVersionUID = 4941026070769773563L;

	public static boolean createTable(Connection conn, String tableName, String createSql) {

		Statement stmt = null;
		boolean success = false;

		try {

			if (!tableExists(conn, tableName)) {

				conn.setAutoCommit(false);

				stmt = conn.createStatement();
				stmt.execute(createSql);

				conn.commit();
				stmt.close();

			}
			success = true;

		} catch (SQLException ignored) {

		} finally {

			if (stmt != null)
				try {
					stmt.close();

				} catch (SQLException ignored) {
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

			int sqlType = metadata.getColumnType(i);			
			int precision = metadata.getPrecision(i);

			int scale = metadata.getScale(i);
			boolean signed = metadata.isSigned(i);
			
			Schema.Type schemaType = getSchemaType(sqlType, precision, scale, signed);	
			if (schemaType != null) {
				
				Schema columnSchema = Schema.of(schemaType);
	
				if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
					columnSchema = Schema.nullableOf(columnSchema);
				}
	
				Schema.Field field = Schema.Field.of(columnName, columnSchema);
				schemaFields.add(field);

			}
		}

		return schemaFields;

	}
	
	public static Schema.Type getSchemaType(int sqlType, int precision, int scale, boolean signed) throws SQLException {

		Schema.Type type = null;
		switch (sqlType) {

		/* BOOLEAN */
		case Types.BOOLEAN:
		case Types.BIT:
			type = Schema.Type.BOOLEAN;
			break;

		/* INT or LONG */
		case Types.INTEGER: {
			
			if (signed) 
				type = Schema.Type.INT; 
			else 
				type = Schema.Type.LONG;
			
			break;
		}
		
		/* INT */
		case Types.SMALLINT:
		case Types.TINYINT:
			type = Schema.Type.INT;
			break;
		
		/* STRING */
		case Types.CHAR:
		case Types.CLOB:
		case Types.LONGNVARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NCLOB:
		case Types.NVARCHAR:
		case Types.REF:
		case Types.SQLXML:
		case Types.STRUCT:
		case Types.VARCHAR:
			type = Schema.Type.STRING;
			break;
		
		case Types.NULL:
			type = Schema.Type.NULL;
			break;

		case Types.BIGINT:
		case Types.ROWID:
			type = Schema.Type.LONG;
			break;

		case Types.REAL:
		case Types.FLOAT:
			type = Schema.Type.FLOAT;
			break;

		case Types.DECIMAL:
		case Types.NUMERIC:
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
			type = null;
		}

		return type;
	}

}
