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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.kp.works.connect.common.JsonUtil;
import de.kp.works.connect.common.http.HttpResponse;
import de.kp.works.connect.common.http.page.HttpPage;

public class WebhosePageIterator implements Iterator<HttpPage>, Closeable {
	
	private final WebhoseConfig config;
	private final WebhoseClient client;
	
	private HttpResponse response;
	/*
	 * URL management 
	 */
	private String nextPageUrl;
	/*
	 * PAGE Management
	 */
	private boolean currentPageReturned = true;
	private HttpPage page;
	
	/*
	 * Webhose provides the next url as part of the result
	 * of the JSON response
	 */
	public WebhosePageIterator(WebhoseConfig config) {
		this.config = config;
		this.client = new WebhoseClient();		
		// TODO
		this.nextPageUrl = null;
		
	}
	
	private String getNextPageUrl(HttpResponse response) {
		
		JsonElement jsonElement = JsonUtil.toJsonElement(response.getBody());
		if (!jsonElement.isJsonObject())
			throw new IllegalStateException("Webhose responses must be Json objects.");

		JsonObject jsonObject = jsonElement.getAsJsonObject();
		WebhoseFormat format = config.getMessageFormat();
		
		return WebhoseUtil.getNextUrl(jsonObject, format);

	}

	private boolean hasNextPage() {

		try {
			/*
			 * This flag indicates that the request (see next())
			 * has received a HttpPage and we have to run for
			 * the next page
			 */
			if (currentPageReturned) {
				
				page = getNextPage();
				currentPageReturned = false;
			
			}
			/* check hasNext() to stop on first empty page. */
			return page != null && page.hasNext();
		
		} catch (Exception e) {
			throw new IllegalStateException("Failed to the load page", e);
		}
		
	}
	
	private HttpPage getNextPage() {
		/*
		 * This guard works because nextPageUrl has been initialized
		 * (see above) within the constructor
		 */
		if (nextPageUrl == null)
			return null;

		try {

			if (response != null) 
				response.close();

			response = new HttpResponse(client.executeHTTP(nextPageUrl));

			if (response.getStatusCode() != 200)
				throw new Exception("Fetching JSON records from Webhose endpoint failed.");
			
		} catch (Exception e) {
			throw new IllegalStateException("Failed to the load page", e);
		}

		HttpPage page = new WebhosePage(config, response);
		nextPageUrl = getNextPageUrl(response);

		return page;
		
	}
	
	@Override
	public HttpPage next() {

		if (!hasNextPage()) {
			throw new NoSuchElementException("No more pages to load.");
		}

		currentPageReturned = true;
		return page;
		
	}

	@Override
	public boolean hasNext() {
		return hasNextPage();
	}

	@Override
	public void close() throws IOException {
		
		try {
			client.close();

		} finally {
			if (response != null) {
				response.close();
			}
		}
		
	}
	
}
