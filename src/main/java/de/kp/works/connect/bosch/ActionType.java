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

public enum ActionType {
	
	CREATE("create"),
	CREATED("created"),
	DELETE("delete"),
	DELETED("deleted"),
	MODIFY("modify"),
	MODIFIED("modified"),
	RETRIEVE("retrieved");

	private final String value;

	ActionType(String value) {
		this.value = value;
	}

	public static Boolean isAction(String action) {
		
		Boolean result = false;
		
		for (ActionType actionType: ActionType.values()) {
	        if (actionType.name().equals(action))
	            result = true;
	    }
		
	    return result;
		
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return this.getValue();
	}

}
