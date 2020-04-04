package de.kp.works.connect.bosch;
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

/**
 * Convenience class to work with Bosch IoT Things / Eclipse Ditto protocol
 * message. Adds some parsing functionality on top of the ThingMessageInfo
 * interface.
 */
public class ThingMessage extends ThingMessageInfo {

	private Boolean _parsed = false;
	private String[] _topicElements;
	private String _thingId;

	public ThingMessage(ThingMessageInfo obj) {

		this.headers = obj.headers;
		this.fields = obj.fields;

		this.topic = obj.topic;
		this.path = obj.path;

		this.status = obj.status;
		this.value = obj.value;

	}

	private void doParse() throws Exception {

		this._topicElements = this.topic.split("/");
		this._thingId = this._topicElements[0] + ':' + this._topicElements[1];

		String group = this._topicElements[2];
		if (!group.equals("thing"))
			throw new Exception(String.format("Topic group '%s' invalid in topic '%s'.", group, this.topic));

		this._parsed = true;

	}

	public String thingId() throws Exception {

		if (!this._parsed)
			this.doParse();

		return this._thingId;

	}

	public String localThingId() throws Exception {

		if (!this._parsed)
			this.doParse();

		return this._topicElements[1];

	}

	public String action() throws Exception {

		if (!this._parsed)
			this.doParse();

		String action = this._topicElements[5];
		if (!ActionType.isAction(action))
			throw new Exception(String.format("Topic action '%s' invalid in topic '%s'.", action, this.topic));

		return action;
	}

	public String channel() throws Exception {

		if (!this._parsed)
			this.doParse();

		String channel = this._topicElements[3];
		if (!ChannelType.isChannel(channel))
			throw new Exception(String.format("Topic channel '%s' invalid in topic '%s'.", channel, this.topic));

		return channel;

	}

	public String criterion() throws Exception {

		if (!this._parsed)
			this.doParse();

		String criterion = this._topicElements[4];
		if (!CriterionType.isCriterion(criterion))
			throw new Exception(String.format("Topic criterion '%s' invalid in topic '%s'.", criterion, this.topic));

		return criterion;

	}

	public String namespace() throws Exception {

		if (!this._parsed)
			this.doParse();

		return this._topicElements[0];

	}

}
