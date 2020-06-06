package de.kp.works.connect.orientdb;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

public class OrientGraphWritable implements OrientWritable {

	private StructuredRecord record;
	/*
	 * It is a prerequisite that the structured record
	 * contains an "id" field
	 */
	public OrientGraphWritable(StructuredRecord record) {
		this.record = record;
	}

	@Override
	public void write(OrientGraphNoTx connection, String vertexType, String edgeType) {
		
		if (connection == null) return;
		
		if (edgeType == null)
			writeAsVertex(connection, vertexType);
		
		else 
			writeAsEdge(connection, edgeType, vertexType);
		
	}
	
	private void writeAsVertex(OrientGraphNoTx connection, String vertexType) {
		/*
		 * We have to create the vertex type here as we can
		 * not ensure that the schema is available earlier
		 */
		Schema schema = getNonNullIfNullable(record.getSchema());
		createVertexType(connection, vertexType, schema);
		/*
		 * The vertex is created from the 'id' field of the
		 * structured record
		 */
		Map<String,Object> props = new HashMap<>();
		props.put("id", record.get("id"));
		
		connection.setStandardElementConstraints(false);
		Vertex vertex = connection.addVertex(String.format("class:%s", vertexType), props);
		
		List<Schema.Field> fields = schema.getFields();
		
		for (Schema.Field field: fields) {
			
			String fieldName = field.getName();
			vertex.setProperty(fieldName, record.get(fieldName));
			
		}
		
	}
	
	private void writeAsEdge(OrientGraphNoTx connection, String edgeType, String vertexType) {
		/*
		 * We have to create the edge type here as we can
		 * not ensure that the schema is available earlier
		 */
		Schema schema = getNonNullIfNullable(record.getSchema());
		createEdgeType(connection, edgeType, schema);
		/*
		 * Determine vertex type of in & out vertices as saving
		 * edges requires the vertexType parameter specified
		 */
		String inType = null;
		String outType = null;
		
		String[] vertexTypes = vertexType.split(",");
		if (vertexTypes.length == 1) {
			inType = vertexTypes[0];
			outType = vertexTypes[0];
		
		} else {
			inType = vertexTypes[0];
			outType = vertexTypes[1];
			
		}
		/*
		 * Process in vertices
		 */
		String inName = (String)record.get("src");
		String outName = (String)record.get("dst");
		/*
		 * We expect that the in & out vertices exist
		 */
		Vertex inVertex = getVertices(connection, inName, inType).get(0);
		Vertex outVertex = getVertices(connection, outName, outType).get(0);
		
		Edge edge = connection.addEdge(null, inVertex, outVertex, edgeType);
		
		List<Schema.Field> fields = schema.getFields();
		
		for (Schema.Field field: fields) {
			
			String fieldName = field.getName();
			edge.setProperty(fieldName, record.get(fieldName));
			
		}
		
	}
	
	private List<Vertex> getVertices(OrientGraphNoTx connection, String vertexName, String vertexType) {
		
		String sql = String.format("select * from %s where id = '%s'", vertexType, vertexName);
		OCommandSQL sqlCmd = new OCommandSQL(sql);

		@SuppressWarnings("unchecked")
		Iterable<Vertex> result = (Iterable<Vertex>) connection.command(sqlCmd).execute();

		List<Vertex> vertices = new ArrayList<>();
		result.forEach(vertices::add);
		
		return vertices;

	}
	
	private void createVertexType(OrientGraphNoTx connection, String vertexTypeName, Schema schema) {
		/*
		 * The user has to make sure that in case of 
		 * an existing vertex type, the provided schema
		 * matches the existing properties
		 */
		if (doesVertexTypeExist(connection, vertexTypeName)) return;
		
		OrientVertexType vertexType = connection.createVertexType(vertexTypeName);
		for (Schema.Field field : schema.getFields()) {
			
			String fieldName = field.getName();
			if (fieldName.equals("id")) {
				
				String indexName = String.format("%sIdx", vertexTypeName);
				vertexType.createIndex(indexName, OClass.INDEX_TYPE.UNIQUE, fieldName);

			} else {
				/*
				 * Field names other than 'id' are handled as
				 * regular vertex type properties.
				 * 
				 * Future implementation may take a linked type
				 * property into account
				 */
				vertexType.createProperty(fieldName, OrientUtil.toOrientType(field.getSchema()));

			}
			
		}
		
	}
	
	private void createEdgeType(OrientGraphNoTx connection, String edgeTypeName, Schema schema) {
		/*
		 * The user has to make sure that in case of 
		 * an existing edge type, the provided schema
		 * matches the existing properties
		 */
		if (doesEdgeTypeExist(connection, edgeTypeName)) return;

		OrientEdgeType edgeType = connection.createEdgeType(edgeTypeName);
		for (Schema.Field field : schema.getFields()) {
			
			String fieldName = field.getName();
			/*
			 * Field names other than 'id' are handled as
			 * regular edge type properties.
			 * 
			 * Future implementation may take a linked type
			 * property into account
			 */
			edgeType.createProperty(fieldName, OrientUtil.toOrientType(field.getSchema()));
			
		}
		
	}
	
	/*
	 * Check whether the vertex type already exists 
	 * in the OrientDB
	 */
	private Boolean doesVertexTypeExist(OrientGraphNoTx connection, String vertexType) {
		return connection.getVertexType(vertexType) != null;
	}
	/*
	 * Check whether the edge type already exists
	 * in the OrientDB
	 */
	private Boolean doesEdgeTypeExist(OrientGraphNoTx connection, String edgeType) {
		return connection.getEdgeType(edgeType) != null;
	}
	
	/*
	 * This is a helper method to determine a certain
	 * vertex by its unique property
	 *
	private Vertex getVertex(OrientGraphNoTx connection, String key, String value) {
		
		Iterator<Vertex> iter = connection.getVertices(key, value).iterator();
		if (iter.hasNext() == false) return null;
		
		return iter.next();

	}
	*/
	private static Schema getNonNullIfNullable(Schema schema) {
		return schema.isNullable() ? schema.getNonNullable() : schema;
	}

}
