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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;

public class JdbcSourceConfig extends JdbcConfig {

	private static final long serialVersionUID = 2348993945279167301L;

	/*
	 * The JdbcSource is made for users with limited data engineering skills and
	 * therefore does not support the following more advanced paramaters that are
	 * useful in a big data environment:
	 * 
	 * bounding query
	 * 
	 * The bounding query is used to determine the amount of data and to separate
	 * them into roughly equivalent shards.
	 * 
	 * split by field
	 * 
	 * The split by field represents a numeric column that is used to separate the
	 * rows into equivalent shards.
	 * 
	 * 
	 */
	@Description("Name of the Jdbc table to import data from.")
	@Nullable
	@Macro
	public String tableName;

	@Description("The SQL select statement to import data from the Jdbc database. "
			+ "For example: select * from <your table name>.")
	@Nullable
	@Macro
	public String inputQuery;

	public String getCountQuery() {
		return String.format("select count(*) from (%s) as input", getInputQuery());
	}
	
	public String getInputQuery() {

		if (Strings.isNullOrEmpty(inputQuery))
			return String.format("select * from %s", tableName);
		
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
	
	public void validate() {

		if (Strings.isNullOrEmpty(tableName) && Strings.isNullOrEmpty(inputQuery))
			throw new IllegalArgumentException(
					"Either table name or query can be empty, however both parameters are empty. "
							+ "This connector is not able to import data from the Jdbc database.");

	}

}
