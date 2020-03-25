package de.kp.works.connect.aerospike;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

/** INPUT SPLIT **/

public class AerospikeSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {

	private String node;
	private Configuration conf;
	
	public AerospikeSplit() {
	}

	public AerospikeSplit(String node, Configuration conf) {
		this.conf = conf;
		this.node = node;
	}
	
	public Configuration getConf() {
		return conf;
	}
	
	public String getNode() {
		return node;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		conf = new Configuration();
		
		String operation = new String(Text.readString(in));
		AerospikeUtil.setOperation(conf, operation);

		node = new String(Text.readString(in));
		
		String host = new String(Text.readString(in));
		AerospikeUtil.setHost(conf, host);
		
		int port = in.readInt();
		AerospikeUtil.setPort(conf, port);
		
		String namespace = new String(Text.readString(in));
		AerospikeUtil.setNamespace(conf, namespace);
		
		String setName = new String(Text.readString(in));
		AerospikeUtil.setSetName(conf, setName);

		int binLen = in.readInt();
		if (binLen == 0)
			AerospikeUtil.setBins(conf, null);
		
		else {
			
			String[] bins = new String[binLen];
			for (int i = 0; i < binLen; i++) {
				bins[i] = new String(Text.readString(in));
			}
			
			AerospikeUtil.setBins(conf, String.join(",", bins));
			
		}
		
		String numRangeBin = new String(Text.readString(in));
		AerospikeUtil.setNumRangeBin(conf, numRangeBin);
		
		Long numRangeBegin = in.readLong();
		AerospikeUtil.setNumRangeBegin(conf, numRangeBegin);
		
		Long numRangeEnd = in.readLong();
		AerospikeUtil.setNumRangeEnd(conf, numRangeEnd);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		/*
		 * - operation
		 * - nodeName
		 * - host
		 * - port
		 * - namespace
		 * - setName
		 * - binLength
		 * - bin ...
		 * - numRangeBin
		 * - numRangeBegin
		 * - numRangeEnd
		 */
		String operation = AerospikeUtil.getOperation(conf);
		Text.writeString(out, operation);
		
		String nodeName = node;
		Text.writeString(out, nodeName);
		
		String host = AerospikeUtil.getHost(conf);
        Text.writeString(out, host);
        
        int port = AerospikeUtil.getPort(conf);
		out.writeInt(port);

		String namespace = AerospikeUtil.getNamespace(conf);
		Text.writeString(out, namespace);
		
		String setName = AerospikeUtil.getSetName(conf);
		Text.writeString(out, setName);

		String[] bins = AerospikeUtil.getBins(conf);
		if (bins == null) {
			out.writeInt(0);
			
		} else {
			
			out.writeInt(bins.length);
			for (String bin : bins)
				Text.writeString(out, bin);
		
		}
		
		String numRangeBin = AerospikeUtil.getNumRangeBin(conf);
		Text.writeString(out, numRangeBin);
		
		Long numRangeBegin = AerospikeUtil.getNumRangeBegin(conf);
		out.writeLong(numRangeBegin);
		
		Long numRangeEnd = AerospikeUtil.getNumRangeEnd(conf);
		out.writeLong(numRangeEnd);

	}

	@Override
	public long getLength() {
		return 1;
	}

	@Override
	public String[] getLocations() {
		return new String[] { node };
	}

}
