package de.kp.works.connect.mqtt;
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

public enum MqttFormat {
	
	/*
	 * The MQTT message is processed as a JSON message and the 
	 * respective schema is inferred from a list of sample
	 * messages
	 */
	DEFAULT("default"),
	/*
	 * The uplink message format of the LoRaWAN The Things Network
	 * 
	 * Uplink messages are sent by end-devices to the Network Server 
	 * relayed by one or many gateways.
	 * 
	 */
	TTN_UPLINK("ttn_uplink");
	
	private final String value;

	MqttFormat(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return this.getValue();
	}

}
