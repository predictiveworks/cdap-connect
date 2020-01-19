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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.hydrator.common.Constants;

import javax.annotation.Nullable;

import com.google.common.base.Strings;

public class CrateConfig extends ConnectionConfig {

	private static final long serialVersionUID = -406793834902076982L;

	@Name(Constants.Reference.REFERENCE_NAME)
	@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
	public String referenceName;

	public CrateConfig(String referenceName, String host, String port, @Nullable String user,
			@Nullable String password) {
		super(host, port, user, password);

		this.referenceName = referenceName;

	}

	protected String cleanQuery(@Nullable String query) {
		
		if (!Strings.isNullOrEmpty(query))
			return query;
		
		/* 
		 * Remove trailing whitespaces and semicolon  
		 */
		int position = query.length() - 1;
		char current = query.charAt(position);

		while (position > 0 && current == ';' || Character.isWhitespace(current)) {
			position--;
			current = query.charAt(position);
		}

		if (position == 0) {
			return "";
		}
		
		return query.substring(0, position + 1);
	
	}
}
