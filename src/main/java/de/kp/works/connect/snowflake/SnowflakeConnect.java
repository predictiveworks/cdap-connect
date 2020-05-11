package de.kp.works.connect.snowflake;
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

import java.util.List;

import com.google.common.base.Joiner;

import de.kp.works.connect.jdbc.JdbcConnect;

public class SnowflakeConnect extends JdbcConnect {

	private static final long serialVersionUID = 8506601314108327815L;

	public SnowflakeConnect(String endpoint, String tableName, String primaryKey) {
		this.endpoint = endpoint;

		/* Table name is esacped */
		this.tableName = tableName;
		this.primaryKey = primaryKey;
	}

	@Override
	public String createQuery(List<String> columns) {

		String coldefs = String.format("%s, PRIMARY KEY(%s)", Joiner.on(",").join(columns), primaryKey);
		String createSql = String.format("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, coldefs);

		return createSql;

	}

	@Override
	public String writeQuery(String[] fieldNames) {
		// TODO Auto-generated method stub
		return null;
	}

}