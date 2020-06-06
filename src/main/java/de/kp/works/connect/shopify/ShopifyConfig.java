package de.kp.works.connect.shopify;
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import de.kp.works.connect.BaseConfig;

public class ShopifyConfig extends BaseConfig {

	private static final long serialVersionUID = -4187403344188896623L;

	@Description("Name of the Shopify shop.")
	@Macro
	public String shopname;

	@Description("Version of the Shopify API.")
	@Macro
	public String version;

	/**
	 * BASIC AUTHENTICATION
	 * 
	 * The ShopifyClient performs basic authentication.
	 */

	@Description("Username for basic authentication.")
	@Macro
	public String username;

	@Description("Password for basic authentication.")
	@Macro
	public String password;

	public void validate() {
		super.validate();
	}

	public String getEndpoint() {
		return String.format("https://%s:%s@%s.myshopify.com/admin/api/%s/", username, password, shopname, version);
	}
}
