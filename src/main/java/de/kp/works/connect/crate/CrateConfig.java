package de.kp.works.connect.crate;
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

import de.kp.works.connect.jdbc.JdbcConfig;

public class CrateConfig extends JdbcConfig {

	private static final long serialVersionUID = -406793834902076982L;

	public CrateConfig() {
	}
	
	public void validate() {
		super.validate();
	}
	
	public String getEndpoint() {
		return String.format(Locale.ENGLISH, "jdbc:crate://%s:%s/", host, port);
	}
	
}
