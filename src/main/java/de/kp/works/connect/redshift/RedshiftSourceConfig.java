package de.kp.works.connect.redshift;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.connect.jdbc.JdbcSourceConfig;

public class RedshiftSourceConfig extends JdbcSourceConfig {

	private static final long serialVersionUID = 6631946981577765660L;

	@Description("Name of the Jdbc database to import data from.")
	@Macro
	public String database;
	
	public void validate() {
		super.validate();
		
		if (Strings.isNullOrEmpty(database)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database name must not be empty.", this.getClass().getName()));
		}
		
	}

	public String getEndpoint() {
		return String.format(Locale.ENGLISH, "jdbc:redshift://%s:%s/%s", host, port, database);
	}

}
