package de.kp.works.connect.things.kafka;
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
import de.kp.works.connect.common.BaseConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;

public class ThingsSinkConfig extends BaseConfig {

	private static final long serialVersionUID = 1401383241435144467L;

	@Description("The host of the Thingsboard server.")
	@Macro
	public String host;

	@Description("The port of the Thingsboard server.")
	@Macro
	public String port;

	@Description("User name. E.g. 'tenant@thingsboard.org'.")
	@Macro
	public String user;

	@Description("Password. E.g. 'tenant'.")
	@Macro
	public String password;

	@Description("The name of the asset.")
	@Macro
	public String assetName;

	@Description("The asset type.")
	@Macro
	public String assetType;

	@Description("The number of assets of the specified asset type to take into account to decide whether "
			+ "an asset with the provided name already exists. Default is 1000.")
	@Macro
	public String assetLimit;
	   
	@Description("A comma-separated list of field names, that describe the features of the sending asset.")
	@Macro
	public String assetFeatures;

	public ThingsSinkConfig() {
		assetLimit = "1000";
	}
	/**
	 * This method determines the basic REST endpoint of the Thingsboard server
	 */
	public String getEndpoint() {
		return String.format("http://%s:%s", host, port);
	}

	public String getAssetEndpoint() {
		return String.format("http://%s:%s/api/asset", host, port);
	}

	public void validate() {
		super.validate();
		
		/* CONNECTION */

		if (Strings.isNullOrEmpty(host)) {
			throw new IllegalArgumentException(
					String.format("[%s] The host of the Thingsboard server must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(port)) {
			throw new IllegalArgumentException(
					String.format("[%s] The port of the Thingsboard server must not be empty.", this.getClass().getName()));
		}
		
		/* CREDENTIALS */
		
		if (Strings.isNullOrEmpty(user)) {
			throw new IllegalArgumentException(
					String.format("[%s] The user name must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(password)) {
			throw new IllegalArgumentException(
					String.format("[%s] The password must not be empty.", this.getClass().getName()));
		}
		
		/* ASSET */
		
		if (Strings.isNullOrEmpty(assetName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The asset field must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(assetType)) {
			throw new IllegalArgumentException(
					String.format("[%s] The asset type must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(assetLimit)) {
			throw new IllegalArgumentException(
					String.format("[%s] The asset limit must not be empty.", this.getClass().getName()));
		}
		
		if (Strings.isNullOrEmpty(assetFeatures)) {
			throw new IllegalArgumentException(
					String.format("[%s] The fields that contain asset properties must not be empty.", this.getClass().getName()));
		}
		
	}

}

