package de.kp.works.connect.http.page;
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
import java.util.Arrays;
import java.util.Iterator;

import de.kp.works.connect.http.HttpResponse;

public abstract class HttpPage implements Closeable, Iterator<HttpEntry> {
	
	protected final HttpResponse httpResponse;

	protected HttpPage(HttpResponse httpResponse) {
		this.httpResponse = httpResponse;
	}

	public int getHash() {
		return Arrays.hashCode(httpResponse.getBytes());
	}
}
