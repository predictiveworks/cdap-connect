package de.kp.works.connect.thingsboard;
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

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.Constants;

public class ThingsboardConfig extends PluginConfig {

	private static final long serialVersionUID = 1401383241435144467L;

	public static final String HOST = "host";
	public static final String PORT = "port";

	public static final String USER = "user";
	public static final String PASSWORD = "password";

	public static final String ASSET_FIELD = "assetField";

	public static final String ASSET_LIMIT = "assetLimit";
	public static final String ASSET_TYPE = "assetType";

	public static final String COLUMNS = "columns";

	@Name(Constants.Reference.REFERENCE_NAME)
	@Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
	public String referenceName;

	@Name(HOST)
	@Description("The host of the Thingsboard server.")
	@Macro
	public String host;

	@Name(PORT)
	@Description("The port of the Thingsboard server.")
	@Macro
	public String port;

	@Name(USER)
	@Description("User name. E.g. 'tenant@thingsboard.org'.")
	@Macro
	public String user;

	@Name(PASSWORD)
	@Description("Password. E.g. 'tenant'.")
	@Macro
	public String password;

	@Name(ASSET_FIELD)
	@Description("The name of the asset.")
	@Macro
	public String assetField;

	@Name(ASSET_TYPE)
	@Description("Asset type.")
	@Macro
	public String assetType;

	@Name(ASSET_LIMIT)
	@Description("Asset limit.")
	@Macro
	public String assetLimit;
	   
	@Name(COLUMNS)
	@Description("A comma-separated list of field names, that describe the sending asset.")
	@Macro
	public String columns;

	public ThingsboardConfig() {
		
	}
	/**
	 * This method determines the basic REST endpoint of the Thingsboard server
	 * 
	 * @return
	 */
	public String getEndpoint() {
		return String.format("http://%s:%s", host, port);
	}

	public String getAssetEndpoint() {
		return String.format("http://%s:%s/api/asset", host, port);
	}

	public void validate() {
		
		if (!Strings.isNullOrEmpty(referenceName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The reference name must not be empty.", this.getClass().getName()));
		}
		
		/** CONNECTION **/

		if (!Strings.isNullOrEmpty(host)) {
			throw new IllegalArgumentException(
					String.format("[%s] The host of the Thingsboard server must not be empty.", this.getClass().getName()));
		}
		
		if (!Strings.isNullOrEmpty(host)) {
			throw new IllegalArgumentException(
					String.format("[%s] The port of the Thingsboard server must not be empty.", this.getClass().getName()));
		}
		
		/** CREDENTIALS **/
		
		if (!Strings.isNullOrEmpty(user)) {
			throw new IllegalArgumentException(
					String.format("[%s] The user name must not be empty.", this.getClass().getName()));
		}
		
		if (!Strings.isNullOrEmpty(password)) {
			throw new IllegalArgumentException(
					String.format("[%s] The password must not be empty.", this.getClass().getName()));
		}
		
		/** ASSET **/
		
		if (!Strings.isNullOrEmpty(assetField)) {
			throw new IllegalArgumentException(
					String.format("[%s] The asset field must not be empty.", this.getClass().getName()));
		}
		
		if (!Strings.isNullOrEmpty(assetType)) {
			throw new IllegalArgumentException(
					String.format("[%s] The asset type must not be empty.", this.getClass().getName()));
		}
		
		if (!Strings.isNullOrEmpty(assetLimit)) {
			throw new IllegalArgumentException(
					String.format("[%s] The asset limit must not be empty.", this.getClass().getName()));
		}
		
		if (!Strings.isNullOrEmpty(columns)) {
			throw new IllegalArgumentException(
					String.format("[%s] The asset limit must not be empty.", this.getClass().getName()));
		}
		
		if (!Strings.isNullOrEmpty(columns)) {
			throw new IllegalArgumentException(
					String.format("[%s] The columns that contain asset values must not be empty.", this.getClass().getName()));
		}
		
	}

}

