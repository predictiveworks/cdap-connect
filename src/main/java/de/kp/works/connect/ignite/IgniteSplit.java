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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;

public class IgniteSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {

	private String name;
	private int index;
	
	public IgniteSplit(String name, int index) {
		this.name = name;
		this.index = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	public String getName() {
		return name;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		name = new String(Text.readString(in));
		index = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		Text.writeString(out, name);
		out.writeInt(index);
		
	}

	@Override
	public long getLength() {
		return 1;
	}

	@Override
	public String[] getLocations() {

		List<String> hostNames = new ArrayList<>();
		
		Ignite ignite = IgniteContext.getInstance().getIgnite();
		if (ignite.configuration().getDiscoverySpi() instanceof TcpDiscoverySpi) {
			
			Collection<ClusterNode> nodes = ignite.affinity(name).mapPartitionToPrimaryAndBackups(index);
			for (ClusterNode node: nodes) {
				
				Collection<InetSocketAddress> socketAddresses = ((TcpDiscoveryNode) node).socketAddresses();
				for (InetSocketAddress socketAddress: socketAddresses) {
					hostNames.add(socketAddress.getHostName());
				}
				
			}

        } else {
        	
			Collection<ClusterNode> nodes = ignite.affinity(name).mapPartitionToPrimaryAndBackups(index);
			for (ClusterNode node: nodes) {
				hostNames.addAll(node.hostNames());
			}
        	
        }

		String[] locations = new String[hostNames.size()];
        locations = hostNames.toArray(locations);
		
		return locations;

	}

}
