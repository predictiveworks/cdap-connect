package de.kp.works.connect.jdbc.saphana;
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

public class SAPHanaConnect extends JdbcConnect {

	private static final long serialVersionUID = 4650893169139299302L;

	public SAPHanaConnect(String endpoint, String tableName, String primaryKey) {
		this.endpoint = endpoint;

		/* Table name is esacped */
		this.tableName = tableName;		
		this.primaryKey = primaryKey;
	}
	
	@Override
	public String createQuery(List<String> columns) {
		/*
		 * The primary key and its data type is already specified as 
		 * part of the colums; also each column name is escaped already
		 */
		String coldefs = String.format("%s", Joiner.on(",").join(columns));
		String createSql = String.format("CREATE ROW TABLE %s (%s)", tableName, coldefs);

		return createSql;
	}

	/*
	 * This method defines an upsert query statement without 
	 * a trailing semicolon; SAP Hana does not escape field 
	 * names in UPSERT statements
	 */
	@Override
	public String writeQuery(String[] fieldNames) {
		/*
		 * We need to insert new rows and update existing ones; therefore SAP's UPSERT
		 * command is used: more details
		 * 
		 * https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.03/en-US/
		 * 20fc06a7751910149892c0d09be21a38.html
		 */
		StringBuilder sb = new StringBuilder();
		sb.append("UPSERT ").append(tableName);
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
		sb.append(" WITH PRIMARY KEY");
		/*
		 * We have to omit the ';' at the end
		 */
		return sb.toString();

	}

}
