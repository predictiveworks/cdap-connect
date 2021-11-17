package de.kp.works.connect.redshift;
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

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import de.kp.works.connect.common.jdbc.JdbcConnect;

public class RedshiftConnect extends JdbcConnect {

	private static final long serialVersionUID = 7240703449616435430L;

	private final String distStyle;
	private final String distKey;
	private final String sortKey;
	/*
	 * __KUP__ Support for distribution style & key,
	 * and sort key added.
	 */
	public RedshiftConnect(RedshiftSinkConfig config) {

		this.endpoint = config.getEndpoint();

		this.tableName = config.tableName;
		this.primaryKey = config.primaryKey;

		this.distStyle = config.getDistStyle();
		this.distKey = config.getDistKey();

		this.sortKey = config.getSortKey();

	}

	@Override
	public String createQuery(List<String> columns) {

		String colDefs = String.format("%s, PRIMARY KEY(%s)", Joiner.on(",").join(columns), primaryKey);

		String distStyleDef = String.format("DISTSTYLE %s", this.distStyle);
		String distKeyDef = "";

		if (!Strings.isNullOrEmpty(this.distKey))
			distKeyDef = String.format("DISTKEY (%s)", this.distKey);

		return String.format(
				"CREATE TABLE IF NOT EXISTS %s (%s) %s %s %s", tableName, colDefs, distStyleDef, distKeyDef, this.sortKey);


	}

	/*
	 * The current implementation of the Redshift Sink connectors supports INSERT
	 * only, i.e. the user to make sure that there are no conflicts with respect to
	 * duplicated primary keys
	 */
	@Override
	public String writeQuery(String[] fieldNames) {

		if (fieldNames == null) {
			throw new IllegalArgumentException("[RedshiftConnect] Field names may not be null");
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
		 * We have to omit the ';' at the end
		 */
		return sb.toString();

	}

}
