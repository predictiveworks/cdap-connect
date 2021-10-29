package de.kp.works.connect.orient;
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

import java.io.IOException;

import de.kp.works.connect.common.EmptyOutputCommiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class OrientOutputFormat<K extends OrientWritable, V> extends OutputFormat<K, V> {

	@Override
	public void checkOutputSpecs(JobContext context) {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
		return new EmptyOutputCommiter();
	}

	public class OrientRecordWriter extends RecordWriter<K, V> {

		private final OrientFactory factory;
		private OrientGraphNoTx connection = null;

		private String vertexType = null;
		private String edgeType = null;
		
		public OrientRecordWriter(OrientFactory factory) {

			this.factory = factory;
			if (this.factory != null)
				this.connection = factory.getConn();
		}

		public void setEdgeType(String edgeType) {
			this.edgeType = edgeType;
		}

		public void setVertexType(String vertexType) {
			this.vertexType = vertexType;
		}
		
		@Override
		public void close(TaskAttemptContext context) throws IOException {
			/*
			 * This method is used to close the connection 
			 * to the OrientDB database
			 */
			try {
				if (factory != null) factory.closeConn();

			} catch (Exception e) {
				throw new IOException(
						String.format("Closing connection to OrientDB failed: %s", e.getLocalizedMessage()));
			}

		}

		@Override
		public void write(K key, V value) throws IOException {
			try {

				key.write(connection, vertexType, edgeType);

			} catch (Exception e) {
				throw new IOException(String.format("Writing record to OrientDB failed: %s", e.getLocalizedMessage()));
			}

		}

	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {
		/*
		 * The configuration has been provided by the Orient Output Format Provider
		 */
		Configuration conf = context.getConfiguration();

		try {
			/*
			 * Extract the connection specific parameters to initiate the OrientDB factory
			 */
			String url = conf.get(OrientUtil.ORIENT_URL);

			String user = conf.get(OrientUtil.ORIENT_USER);
			String password = conf.get(OrientUtil.ORIENT_PASSWORD);

			OrientFactory factory = new OrientFactory(url, user, password);
			/*
			 * Next we have to check whether this writer persists vertices or edges; note,
			 * previous checks have validated that one of these write modes is specified
			 */
			String edgeType = conf.get(OrientUtil.ORIENT_EDGE_TYPE);
			String vertexType = conf.get(OrientUtil.ORIENT_VERTEX_TYPE);

			String writeOption = conf.get(OrientUtil.ORIENT_WRITE);
			
			boolean doSave = true;
			/*
			 * The parameters edge type and vertex type have to be provided as follows:
			 * 
			 * a) the dataset contains vertices only
			 * 
			 * - edgeType    = null
			 * - vertexType != null (has be checked earlier)
			 * 
			 * b) the dataset contains edges only
			 * 
			 * - edgeType   != null
			 * - vertexType != null
			 * 
			 */
			if (edgeType == null) {
				/*
				 * The provided structured data records describe vertices
				 */
				switch (writeOption) {
				case "Append": {
					break;
				}
				case "Overwrite": {
					/*
					 * We drop all vertices that refer to the provided vertex 
					 * type and finally also remove the type
					 */
					factory.dropVertexType(vertexType);
					break;
				}
				case "ErrorIfExists": {

					if (factory.doesVertexTypeExist(vertexType))
						throw new IllegalArgumentException(
								String.format("Vertex type '%s' already exists.", vertexType));

					break;
				}
				case "Ignore": {
					if (factory.doesVertexTypeExist(vertexType))
						doSave = false;

					break;
				}
				default:
				}

			} else {
				/*
				 * The provided structured data records describe edges
				 */				
				switch (writeOption) {
				case "Append": {
					break;
				}
				case "Overwrite": {
					/*
					 * We drop all edges that refer to the provided edge 
					 * type and finally also remove the type
					 */
					factory.dropEdgeType(edgeType);
					break;

				}
				case "ErrorIfExists": {
					if (factory.doesEdgeTypeExist(edgeType))
						throw new IllegalArgumentException(String.format("Edge type '%s' already exists.", edgeType));

					break;
				}
				case "Ignore": {
					if (factory.doesEdgeTypeExist(edgeType))
						doSave = false;
					
					break;
				}
				default:
				}
				
			} 
			
			if (doSave) {
				
				OrientRecordWriter writer = new OrientRecordWriter(factory);
				
				writer.setEdgeType(edgeType);
				writer.setVertexType(vertexType);
				
				return writer;

			} else {
				return new OrientRecordWriter(null);
			}

		} catch (Exception e) {
			throw new IOException(
					String.format("Instantiating RecordWriter for OrientDB failed: %s", e.getLocalizedMessage()));
		}

	}

}
