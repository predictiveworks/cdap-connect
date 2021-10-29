package de.kp.works.connect.shopify;
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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.kp.works.connect.common.http.HttpResponse;
import de.kp.works.connect.common.http.page.HttpPage;
import org.apache.http.Header;
import org.apache.http.HeaderElement;

public class ShopifyPageIterator implements Iterator<HttpPage>, Closeable {
	
	private final ShopifyConfig config;
	private final ShopifyClient client;
	
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
	 * Sample of a response header link:
	 * 
	 * 	Link: "<https://{shop}.myshopify.com/admin/api/{version}/products.json?page_info={page_info}&limit={limit}>; rel={next}, 
	 * <https://{shop}.myshopify.com/admin/api/{version}/products.json?page_info={page_info}&limit={limit}>; rel={previous}"
	 */
	private static final Pattern nextPattern = Pattern.compile("<(.+)>; rel=next");
	
	public ShopifyPageIterator(ShopifyConfig config) {
		
		this.config = config;
		this.client = new ShopifyClient();		
		// TODO
		this.nextPageUrl = null;
		
	}

	/*
	 * REST endpoints support cursor-based pagination. When you send a request 
	 * to one of these endpoints, the response body returns the first page of 
	 * results, and a response header returns links to the next page and the
	 *  previous page of results (if applicable). 
	 */
	private String getNextPageUrl(HttpResponse response) {
		
		Header link = response.getFirstHeader("Link");
		
		if (link == null) {
			return null;
		}

		for (HeaderElement headerElement : link.getElements()) {
			
			Matcher matcher = nextPattern.matcher(headerElement.toString());
			if (matcher.matches()) {
				return matcher.group(1);
			}
		
		}

		return null;
		
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
				throw new Exception("Fetching JSON records from Shopify ADMIN endpoint failed.");
			
		} catch (Exception e) {
			throw new IllegalStateException("Failed to the load page", e);
		}

		HttpPage page = new ShopifyPage(config, response);
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
