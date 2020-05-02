package de.kp.works.connect.webhose;
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

import com.google.gson.JsonObject;

public class WebhoseUtil {

	public static String getNextUrl(JsonObject jsonResponse, WebhoseFormat format) {
		
		String nextUrl = null;
		switch(format) {
		case CYBER: {
			/* The Consumption Info section */
			nextUrl = jsonResponse.get("next").getAsString();
			break;			
		}
		case REVIEW: {
			/* The Meta Info section */
			nextUrl = jsonResponse.get("next").getAsString();
			break;						
		}
		case WEB: {
			/* The Meta Info section */
			nextUrl = jsonResponse.get("next").getAsString();
			break;
		}
		}
		
		return nextUrl;
	}
	
	public static Integer getTotal(JsonObject jsonResponse, WebhoseFormat format) {
		
		Integer total = 0;
		switch(format) {
		case CYBER: {
			/* The Consumption Info section */
			total = jsonResponse.get("totalResults").getAsInt();
			break;			
		}
		case REVIEW: {
			/* The Meta Info section */
			total = jsonResponse.get("totalResults").getAsInt();
			break;						
		}
		case WEB: {
			/* The Meta Info section */
			total = jsonResponse.get("totalResults").getAsInt();
			break;
		}
		}
		
		return total;
	
		
	}
}
