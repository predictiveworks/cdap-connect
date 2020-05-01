package de.kp.works.connect.jdbc;
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.connect.BaseConfig;

import javax.annotation.Nullable;

import com.google.common.base.Strings;

public class JdbcConfig extends BaseConfig {
	
	private static final long serialVersionUID = -6872662164868638935L;

	@Description("The host of the database.")
	@Macro
	public String host;

	@Description("The port of the database.")
	@Macro
	public String port;

	@Description("User name. Required for databases that need authentication. Optional otherwise.")
	@Nullable
	@Macro
	public String user;

	@Description("Password. Required for databases that need authentication. Optional otherwise.")
	@Nullable
	@Macro
	public String password;
	
	public JdbcConfig() {
	}

	public void validate() {
		super.validate();
		
		if (Strings.isNullOrEmpty(host)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database host must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(port)) {
			throw new IllegalArgumentException(
					String.format("[%s] The database port must not be empty.", this.getClass().getName()));
		}
		
	}
	
}
