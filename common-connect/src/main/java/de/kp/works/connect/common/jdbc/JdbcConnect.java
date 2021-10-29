package de.kp.works.connect.common.jdbc;
/*
 * Copyright (c) 2019 - 2ß21 Dr. Krusche & Partner PartG. All rights reserved.
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
import java.sql.*;
import java.util.List;

public abstract class JdbcConnect implements Serializable {

	private static final long serialVersionUID = -5665668362753941685L;

	protected int[] columnTypes = null;

	protected String endpoint;
	
	protected String tableName;
	protected String primaryKey;

	public abstract String createQuery(List<String> columns);
	
	public boolean createTable(Connection conn, List<String> columns) {
		return createTable(conn, tableName, primaryKey, columns);
	}

	private boolean createTable(Connection conn, String tableName, String primaryKey, List<String> columns) {

		boolean success = false;

		try {

			String createSql = createQuery(columns);
			success = JdbcUtils.createTable(conn, tableName, createSql);

		} catch (Exception ignored) {
		}

		return success;

	}
	
	public abstract String writeQuery(String[] fieldNames);
	
	public int[] getColumnTypes() {
		return columnTypes;
	}

	public void loadColumnTypes(Connection conn) {
		loadColumnTypes(conn, tableName);
	}

	private void loadColumnTypes(Connection conn, String tableName) {

		Statement stmt = null;

		try {

			stmt = conn.createStatement();
			/*
			 * Run a query against the DB table that returns 0 records, but returns valid
			 * ResultSetMetadata that can be used to optimize write requests to the Crate
			 * database
			 */
			ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s WHERE 1 = 0", tableName));
			ResultSetMetaData rsMetadata = rs.getMetaData();

			int columnCount = rsMetadata.getColumnCount();

			columnTypes = new int[columnCount];
			for (int i = 0; i < columnCount; i++) {
				columnTypes[i] = rsMetadata.getColumnType(i + 1);
			}

		} catch (Exception ignored) {
		} finally {

			if (stmt != null)
				try {
					stmt.close();

				} catch (SQLException ignored) {
				}

		}

	}

	public String getEndpoint() {
		return endpoint;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public String getTableName() {
		return tableName;
	}

}
