package org.researchgraph.neo4j.interfaces;

import org.neo4j.graphdb.Node;
import org.researchgraph.neo4j.Neo4jDatabase.Neo4jTransaction;

public interface ProcessNode {
	boolean processNode(Neo4jTransaction tx, Node node) throws Exception;
}
