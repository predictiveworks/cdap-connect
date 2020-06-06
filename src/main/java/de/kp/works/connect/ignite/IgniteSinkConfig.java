package de.kp.works.connect.ignite;
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

import java.util.Properties;

import javax.annotation.Nullable;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

public class IgniteSinkConfig extends IgniteConfig {
	
	private static final long serialVersionUID = 3308508419494049261L;

	@Description("The cache mode used when the provided cache does not exist. Supported values "
			+ "are 'partitioned' and 'replicated'. Default is 'partitioned'.")
	@Macro
	@Nullable
	public String cacheMode;

	public IgniteSinkConfig() {
		super();
		
		cacheMode = "partitioned";
		
	}
	
	public void validate() {
		super.validate();
	}
	
	@Override
	public Properties getConfig() {

		Properties config = super.getConfig();

		config.setProperty(IgniteUtil.IGNITE_CACHE_MODE, cacheMode);
		return config;

	}

}
