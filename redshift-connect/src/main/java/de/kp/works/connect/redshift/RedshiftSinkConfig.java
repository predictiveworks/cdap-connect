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

import java.util.Locale;

import com.google.common.base.Strings;

import de.kp.works.connect.common.jdbc.JdbcSinkConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;

public class RedshiftSinkConfig extends JdbcSinkConfig {

	private static final long serialVersionUID = 7023854483348316577L;

	private final String DIST_STYLE_DESC = "The Redshift Distribution Style to be used when creating a table." +
			" Can be one of EVEN, KEY or ALL (see Redshift docs). When using KEY, you must also set a distribution" +
			" key.";

	private final String SORT_KEY_DESC = "A full Redshift Sort Key definition. Examples include:" +
			" SORTKEY(my_sort_column), COMPOUND SORTKEY(sort_col_1, sort_col_2), INTERLEAVED SORTKEY(sort_col_1, sort_col_2).";

	@Description("Name of the database to import data from.")
	@Macro
	public String database;

	@Description("The name of a column in the table to use as the distribution key when creating a table.")
	@Macro
	@Nullable
	public String distKey;

	@Description(DIST_STYLE_DESC)
	@Macro
	public String distStyle;

	@Description(SORT_KEY_DESC)
	@Macro
	@Nullable
	public String sortKey;

	public String getDistStyle() {
		if  (Strings.isNullOrEmpty(distStyle)) return "EVEN";
		return distStyle;
	}

	@Nullable
	public String getDistKey() {
		return distKey;
	}

	public String getSortKey() {
		if  (Strings.isNullOrEmpty(distStyle)) return "";
		return sortKey;
	}

	public String getEndpoint() {
		return String.format(Locale.ENGLISH, "jdbc:redshift://%s:%s/%s", host, port, database);
	}
	
	public void validate() {
		super.validate();
		
		if (Strings.isNullOrEmpty(database)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database name must not be empty.", this.getClass().getName()));
		}

		if (getDistStyle().equals("KEY") && Strings.isNullOrEmpty(distKey)) {
			throw new IllegalArgumentException(
					String.format("[%s] The distribution key must not be empty for style = KEY.", this.getClass().getName()));
		}
		
	}


}
