package de.kp.works.connect.common.http.error;
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

import de.kp.works.connect.common.http.core.ValuedEnum;

/**
 * Specifies how to respond to an eeror while reading
 * HTTP records.
 */
public enum ErrorStrategy implements ValuedEnum {

	SUCCESS("success"),

	SKIP("skip"),

	SEND("send"),

	STOP("stop");

	private final String value;

	ErrorStrategy(String value) {
		this.value = value;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return this.getValue();
	}
}
