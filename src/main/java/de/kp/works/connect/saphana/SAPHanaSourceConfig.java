package de.kp.works.connect.saphana;
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

import java.util.Locale;

import com.google.common.base.Strings;

import de.kp.works.connect.jdbc.JdbcSourceConfig;

public class SAPHanaSourceConfig extends JdbcSourceConfig {

	private static final long serialVersionUID = 773492290391412814L;

	private static final Character ESCAPE_CHAR = '"';
	
	@Override
	public String getInputQuery() {

		if (Strings.isNullOrEmpty(inputQuery)) {
			/*
			 * Check whether the table name is escaped
			 */
			String table = tableName;
			if (table.charAt(0) != ESCAPE_CHAR)
				table = ESCAPE_CHAR + table;
			
			if (table.charAt(table.length() -1) != ESCAPE_CHAR)
				table = table + ESCAPE_CHAR;
			
			return String.format("select * from %s", table);
	
		}
		/* Remove (trailing) whitespace */
		String cleaned = inputQuery.trim();

		/* Remove trailing semicolon */
		int position = cleaned.length() - 1;
		char current = cleaned.charAt(position);
		
		if (current == ';') {
			cleaned = cleaned.substring(0, position);
		}
		
		return cleaned;
	
	}

	public String getEndpoint() {
		return String.format(Locale.ENGLISH, "jdbc:sap://%s:%s/", host, port);
	}

}
