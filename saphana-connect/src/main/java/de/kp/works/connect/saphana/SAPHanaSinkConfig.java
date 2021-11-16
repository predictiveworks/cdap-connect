package de.kp.works.connect.saphana;
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

import com.google.common.base.Strings;
import de.kp.works.connect.common.jdbc.JdbcSinkConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;
import java.util.Locale;

public class SAPHanaSinkConfig extends JdbcSinkConfig {

	private static final long serialVersionUID = -343935437395444858L;
	private static final Character ESCAPE_CHAR = '"';

	@Description("Name of the database table to import data from.")
	@Macro
	@Nullable
	public String database;

	public String getEndpoint() {
		if (Strings.isNullOrEmpty(database))
			return String.format(Locale.ENGLISH, "jdbc:sap://%s:%s/", host, port);
		else
			return String.format(Locale.ENGLISH, "jdbc:sap://%s:%s/?%s", host, port, database);
	}
	
	public String getTableName() {

		String table = tableName;
		if (table.charAt(0) != ESCAPE_CHAR)
			table = ESCAPE_CHAR + table;

		if (table.charAt(table.length() - 1) != ESCAPE_CHAR)
			table = table + ESCAPE_CHAR;
		
		return table;
	}

}
