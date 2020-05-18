package de.kp.works.connect.iot.mqtt;
/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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

import java.io.Serializable;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import de.kp.works.connect.SslConfig;

public class HiveMQConfig extends SslConfig implements Serializable {

	private static final long serialVersionUID = 3127652226872012920L;

	private static final String USER_DESC = "The MQTT user name.";
	
	private static final String PASSWORD_DESC = "The MQTT user password.";
	
	@Description(USER_DESC)
	@Macro
	public String mqttUser;

	@Description(PASSWORD_DESC)
	@Macro
	public String mqttPassword;

}
