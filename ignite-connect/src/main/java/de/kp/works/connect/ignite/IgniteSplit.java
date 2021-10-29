package de.kp.works.connect.ignite;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;

public class IgniteSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {

	private long end = 0;
	private long start = 0;

	public IgniteSplit(long start, long end) {
		this.start = start;
		this.end = end;
	}

	public Long getStart() {
		return start;
	}

	public Long getEnd() {
		return end;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		start = in.readLong();
		end = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(start);
		out.writeLong(end);
	}

	@Override
	public long getLength() {
		return end - start;
	}

	@Override
	public String[] getLocations() {
		return new String[] {};

	}

}
