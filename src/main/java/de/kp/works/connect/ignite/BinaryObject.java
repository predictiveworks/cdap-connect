package de.kp.works.connect.ignite;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BinaryObject implements Writable {

	private org.apache.ignite.binary.BinaryObject object;
	
	public BinaryObject() {
		
	}
	
	public BinaryObject(org.apache.ignite.binary.BinaryObject object) {
		this.object = object;
	}
	
	public Object getField(String fieldName) {
		return this.object.field(fieldName);
	}
	
	public org.apache.ignite.binary.BinaryObject getObject() {
		return object;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		throw new IOException("Method 'readField' is not supported.");
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		throw new IOException("Method 'write' is not supported.");
	}

}
