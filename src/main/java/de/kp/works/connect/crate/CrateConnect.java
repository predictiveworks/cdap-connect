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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class CrateConnect {

	private static final Logger LOG = LoggerFactory.getLogger(CrateConnect.class);

	private String host;
	private String port;
	
	private String user;
	private String password;
	
	private String tableName;
	private String primaryKey;
	
	private String inputQuery;
	
	private int[] columnTypes;
		
	public CrateConnect() {
		
	}

	public CrateConnect setHost(String host) {
		this.host = host;
		return this;
	}

	public CrateConnect setPort(String port) {
		this.port = port;
		return this;
	}

	public CrateConnect setUser(String user) {
		this.user = user;
		return this;
	}

	public CrateConnect setPassword(String password) {
		this.password = password;
		return this;
	}

	public CrateConnect setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public CrateConnect setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
		return this;
	}

	public String getInputQuery() {
		return inputQuery;
	}

	public CrateConnect setInputQuery(String inputQuery) {
		this.inputQuery = inputQuery;
		return this;
	}
	
	public Connection getConnection() throws SQLException {

		Connection conn;		
		if (user == null || password == null) {
			conn = DriverManager.getConnection(getEndpoint());

		} else {
			conn = DriverManager.getConnection(getEndpoint(), user, password);

		}

		return conn;

	}

	public int[] getColumnTypes() {
		return columnTypes;
	}

	public void loadColumnTypes() throws Exception {
		loadColumnTypes(tableName);
	}
	/**
	 * The column types are required for each incoming
	 * record when it is transformed into an insert stmt;
	 * 
	 * therefore, we load them right after a new data table
	 * has been created 
	 */
	public void loadColumnTypes(String tableName) throws Exception {

		Connection conn = null;
		Statement stmt = null;

		int[] columnTypes;
		
		try {

			conn = getConnection();
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
			/*
			 * Publish the derived column types and make them available for further processing
			 */
			this.columnTypes = columnTypes;			
			LOG.info(String.format("Column definitions found for %s columns." + columnTypes.length));
			
		} catch (Exception e) {
			LOG.error(String.format("Retrieval of column types failed with: %s", e.getLocalizedMessage()));

		} finally {

			if (stmt != null) stmt.close();
			if (conn != null) conn.close();

		}

	}

	public String insertQuery(String[] fieldNames) {
		return insertQuery(tableName, fieldNames);
	}
	/*
	 * This method builds a Crate compliant insert query that is used
	 * to feed a SQL prepared statement that finally write a record to
	 * the database
	 */
	public String insertQuery(String tableName, String[] fieldNames) {

		if (fieldNames == null) {
			throw new IllegalArgumentException("[CrateConnect] Field names may not be null");
		}

		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ").append(tableName);
		/*
		 * Append column block
		 */
		sb.append(" (");
		for (int i = 0; i < fieldNames.length; i++) {
			sb.append(fieldNames[i]);
			if (i != fieldNames.length - 1) {
				sb.append(",");
			}
		}
		sb.append(")");
		/*
		 * Append binding block
		 */
		sb.append(" VALUES (");

		for (int i = 0; i < fieldNames.length; i++) {
			sb.append("?");
			if (i != fieldNames.length - 1) {
				sb.append(",");
			}
		}

		sb.append(")");
		/*
		 * Append duplicate block; it is important
		 * to note, that the KEY must be excluded 
		 * from this block
		 */
		sb.append(" ON DUPLICATE KEY UPDATE ");
		for (int i = 0; i < fieldNames.length; i++) {
			
			if (fieldNames[i].equals(primaryKey)) continue;

			sb.append(fieldNames[i] + "=VALUES(" + fieldNames[i] + ")");
			if (i != fieldNames.length - 1) {
				sb.append(",");
			}

		}
		/*
		 * We have to omit the ';' at the end
		 */
		return sb.toString();		

	}
	
	public String createQuery(String tableName, String primaryKey, List<String> columns) {

		String coldefs = String.format("%s, PRIMARY KEY(%s)", Joiner.on(",").join(columns), primaryKey);
		/*
		 * A simple, but very important tweak to speed up importing is to set the
		 * refresh interval of the table to 0. This will disable the periodic refresh of
		 * the table that is needed to minimise the effect of eventual consistency and
		 * therefore also minimise the overhead during import.
		 * 
		 * Lessons learned: This configuration prevents read-after-write which is also
		 * an important feature. As a trade off, we explicitly refresh the data after
		 * import.
		 * 
		 * Lessons learned: Refresh initiates a table reader and requires the table to
		 * be 'visible'; this, however, is achieved after having refreshed the table.
		 */
		String createSql = String.format("CREATE TABLE IF NOT EXISTS %s (%s) WITH (refresh_interval = 0)", tableName,
				coldefs);
		
		return createSql;
	
	}
	
	public boolean createTable(List<String> columns) {
		return createTable(tableName, primaryKey, columns);
	}
	/**
	 * This method creates a Crate table with provided name, primary key
	 * and specified columns, if it does not exit;  
	 */
	public boolean createTable(String tableName, String primaryKey, List<String> columns) {

		try {

			String createSql = createQuery(tableName, primaryKey, columns);

			Connection conn = null;
			Statement stmt = null;

			try {

				conn = getConnection();
				if (tableExists(conn,tableName) == false) {

					conn.setAutoCommit(false);

					stmt = conn.createStatement();
					stmt.execute(createSql);

					conn.commit();
					stmt.close();
					
				}

			} catch (Exception e) {
				LOG.error(String.format("[CrateConnect] The creation of table '%s' failed with: %s", tableName, e.getLocalizedMessage()));

			} finally {

				if (stmt != null) stmt.close();
				if (conn != null) conn.close();

			}

			return true;

		} catch (Exception e) {
			e.printStackTrace();
			return false;

		}
	}
	
	public Boolean tableExists(Connection conn, String table) throws SQLException {
		
	    DatabaseMetaData metadata = conn.getMetaData();

	    ResultSet rs = metadata.getTables(null, null, table, null);
	    return (rs.next());

	}

	public String getEndpoint() {
		return String.format(Locale.ENGLISH, "jdbc:crate://%s:%s/", host, port);
	}

}
