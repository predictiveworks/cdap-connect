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

public class JdbcSinkConfig extends JdbcConfig {

	private static final long serialVersionUID = 5677211461839972980L;

	@Description("Name of the Jdbc table to export data to.")
	@Macro
	public String tableName;

	@Description("Name of the primary key of the Jdbc table to export data to.")
	@Macro
	public String primaryKey;

	public JdbcSinkConfig() {
		super();
	}

	public void validate() {
		super.validate();

		if (Strings.isNullOrEmpty(tableName))
			throw new IllegalArgumentException(
					"Table name must not be empty. This connector is not able to write data to the Jdbc database.");

		if (Strings.isNullOrEmpty(primaryKey))
			throw new IllegalArgumentException(
					"Primary key must not be empty. This connector is not able to write data to the Jdbc database.");
		
	}
	
}
