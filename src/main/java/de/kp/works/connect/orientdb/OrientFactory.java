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
import java.util.List;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class OrientFactory {

	private OrientGraphFactory db;
	private OrientGraphNoTx conn;
	/*
	 * The url is of the format remote:192.168.163.10 (or any other IP address)
	 */
	public OrientFactory(String url, String user, String password) {
		db = new OrientGraphFactory(url, user, password);
	}
	
	public OrientGraphNoTx getConn() {
		
		conn = db.getNoTx();
		return conn;
		
	}

	public void dropVertexType(String vertexTypeName) {

		if (conn == null)
			conn = getConn();

		if (conn.getVertexType(vertexTypeName) != null) {
			/*
			 * Delete all vertices that are described
			 * by the vertex type first and then delete
			 * the type
			 */
			deleteVertices(vertexTypeName);
			conn.dropVertexType(vertexTypeName);
		}

	}

	public void dropEdgeType(String edgeTypeName) {

		if (conn == null)
			conn = getConn();

		if (conn.getEdgeType(edgeTypeName) != null) {
			/*
			 * Delete all edges that are described
			 * by the edge type first and then delete
			 * the type
			 */
			deleteEdges(edgeTypeName);
			conn.dropEdgeType(edgeTypeName);

		}
	}

	private void deleteVertices(String vertexTypeName) {

		String sql = String.format("select * from %s", vertexTypeName);
		OCommandSQL sqlCmd = new OCommandSQL(sql);

		@SuppressWarnings("unchecked")
		Iterable<Vertex> result = (Iterable<Vertex>) conn.command(sqlCmd).execute();

		List<Vertex> vertices = new ArrayList<>();
		result.forEach(vertices::add);

		for (Vertex vertex : vertices) {
			conn.removeVertex(vertex);
		}

	}
	
	private void deleteEdges(String edgeTypeName) {

		String sql = String.format("select * from %s", edgeTypeName);
		OCommandSQL sqlCmd = new OCommandSQL(sql);

		@SuppressWarnings("unchecked")
		Iterable<Edge> result = (Iterable<Edge>) conn.command(sqlCmd).execute();

		List<Edge> edges = new ArrayList<>();
		result.forEach(edges::add);

		for (Edge edge : edges) {
			conn.removeEdge(edge);
		}
	}
	
	/*
	 * Check whether the vertex type already exists 
	 * in the OrientDB
	 */
	public Boolean doesVertexTypeExist(String vertexTypeName) {

		Boolean exists = conn.getVertexType(vertexTypeName) != null;
		return exists;

	}
	
	/*
	 * Check whether the edge type already exists
	 * in the OrientDB
	 */
	public Boolean doesEdgeTypeExist(String edgeTypeName) {
		
		boolean exists = conn.getEdgeType(edgeTypeName) != null;
		return exists;
		
	}
	
	public void closeConn() {
		db.close();
	}

}
