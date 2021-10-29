package de.kp.works.connect.common.http.page;
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

import de.kp.works.connect.common.http.error.ErrorStrategy;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.InvalidEntry;

public class HttpEntry {
	
	private final StructuredRecord record;
	private final InvalidEntry<StructuredRecord> error;
	
	private final ErrorStrategy strategy;

	public HttpEntry(StructuredRecord record) {

		this.record = record;
		
		this.error = null;
		this.strategy = null;
	}

	public HttpEntry(InvalidEntry<StructuredRecord> error, ErrorStrategy strategy) {

		this.record = null;
		
		this.error = error;
		this.strategy = strategy;
	
	}

	public boolean isError() {
		return error != null;
	}

	public StructuredRecord getRecord() {
		return record;
	}

	public InvalidEntry<StructuredRecord> getError() {
		return error;
	}

	public ErrorStrategy getStrategy() {
		return strategy;
	}
}
