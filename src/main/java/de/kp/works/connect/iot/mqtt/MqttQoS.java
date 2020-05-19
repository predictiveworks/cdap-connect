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

public enum MqttQoS {
	
	/*
	 * The MQTT protocol provides three qualities of service for delivering 
	 * messages between clients and servers: "at most once", "at least once" 
	 * and "exactly once".
	 *
	 * Quality of service (QoS) is an attribute of an individual message being 
	 * published. An application sets the QoS for a specific message by setting 
	 * the MQTTClient_message.qos field to the required value.
	 * 
	 * A subscribing client can set the maximum quality of service a server uses 
	 * to send messages that match the client subscriptions. The QoS of a message 
	 * forwarded to a subscriber thus might be different to the QoS given to the 
	 * message by the original publisher. 
	 * 
	 * The lower of the two values is used to forward a message. The three levels 
	 * are:
	 * 
	 * At most once: The message is delivered at most once, or it may not be delivered 
	 * at all. Its delivery across the network is not acknowledged. The message is not 
	 * stored. 
	 * 
	 * The message could be lost if the client is disconnected, or if the server fails. 
	 * This is the fastest mode of transfer. It is sometimes called "fire and forget".
	 * 
	 * The MQTT protocol does not require servers to forward publications at this level 
	 * to a client. If the client is disconnected at the time the server receives the 
	 * publication, the publication might be discarded, depending on the server implementation.
	 */
	AT_MOST_ONCE("at_most_once"),
	/*
	 * At least once: The message is always delivered at least once. It might be delivered 
	 * multiple times if there is a failure before an acknowledgment is received by the sender. 
	 * The message must be stored locally at the sender, until the sender receives confirmation 
	 * that the message has been published by the receiver. The message is stored in case the
	 * message must be sent again.
	 */
	AT_LEAST_ONCE("at_least_once"),
	/*
	 * Exactly once: The message is always delivered exactly once. The message must be stored 
	 * locally at the sender, until the sender receives confirmation that the message has been 
	 * published by the receiver. The message is stored in case the message must be sent again. 
	 * 
	 * This level is the safest, but slowest mode of transfer. A more sophisticated handshaking 
	 * and acknowledgement sequence is used than for AT_LEAST_ONCE to ensure no duplication of 
	 * messages occurs.
	 */
	EXACTLY_ONCE("exactly_once");

	private final String value;

	MqttQoS(String value) {
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
