package de.kp.works.connect.iot.hivemq;
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

import com.google.common.base.Strings;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;

public class HiveMQSourceConfig extends HiveMQConfig {

	private static final long serialVersionUID = 3127652226872012920L;

	@Description("The comma-separated list of MQTT topics to listen to.")
	@Macro
	public String mqttTopics;

	public void validate() {
		super.validate();

		String className = this.getClass().getName();
		
		if (Strings.isNullOrEmpty(mqttTopics)) {
			throw new IllegalArgumentException(
					String.format("[%s] The MQTT topics must not be empty.", className));
		}
		
	}
	
	public String[] getTopics() {
		
		String[] tokens = mqttTopics.split(",");
		String[]topics = new String[tokens.length];
		
		for (int i = 0; i < tokens.length; i++) {
			topics[i] = tokens[i].trim();
		}
		
		return topics;
		
	}
	
}
