package org.researchgraph.neo4j;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.researchgraph.graph.Graph;
import org.researchgraph.graph.GraphIndex;
import org.researchgraph.graph.GraphKey;
import org.researchgraph.graph.GraphNode;
import org.researchgraph.graph.GraphRelationship;
import org.researchgraph.graph.GraphSchema;
import org.researchgraph.graph.interfaces.GraphImporter;
import org.researchgraph.neo4j.interfaces.ProcessNode;

public class Neo4jDatabase implements GraphImporter {
	private static final String COLUMN_N = "n";
	private static final String NEO4J_CONF = "/conf/neo4j.conf";
	private static final String NEO4J_DB = "/data/databases/graph.db";
		
	private Neo4jTransaction tx;
	
	//private Map<String, Index<Node>> indexes = new HashMap<String, Index<Node>>();
	
	private boolean verbose = false;
	private long nodesCreated = 0;
	private long nodesUpdated = 0;
	private long relationshipsCreated = 0;
	private long relationshipsUpdated = 0;
	
	private final Map<String, List<GraphRelationship>> unknownRelationships = new HashMap<String, List<GraphRelationship>>();
	private final Set<GraphSchema> importedSchemas = new HashSet<GraphSchema>();	
	
	public class Neo4jTransaction {
		final GraphDatabaseService graphDb;
		
		public ConstraintDefinition createConstrant(GraphIndex index) {
			return createConstrant(index.getLabel(), index.getProperty());
		}
		
		public IndexDefinition createIndex(GraphIndex index) {
			return createIndex(index.getLabel(), index.getProperty());
		}
		
		public void importSchemas(Collection<GraphSchema> schemas) {
			if (null != schemas)
				for (GraphSchema schema : schemas) 
					importSchema(schema);
		}
		
		public void importSchema(GraphSchema schema) {
			// make sure we had imported each schema only once
			if (!importedSchemas.contains(schema)) {
				GraphIndex index = schema.getIndex();
				
				if (schema.isUnique()) {
					if (verbose) {
						System.out.println("Creating Constraint {index=" + index + "}");
					}
					createConstrant(index);
				} else {
					if (verbose) {
						System.out.println("Creating Index {index=" + index + "}");
					}
		
					createIndex(index);
				}
				
				importedSchemas.add(schema);
			}
		}
		
		public void importNodes(Collection<GraphNode> nodes) {
			// Import nodes
			if (null != nodes)
				for (GraphNode graphNode : nodes) 
					importNode(graphNode);		
		}
		
		public void importIndex(Node node, GraphKey key) {
			node.addLabel(Label.label(key.getIndex().getLabel()));
			node.setProperty(key.getIndex().getProperty(), key.getValue());
			
			importRelationships(unknownRelationships.remove(getRelationshipKey(key)), false); 
		}
		
		public void importIndexes(Node node, Collection<GraphKey> indexes) {
			if (null != indexes) {
				indexes.stream().forEach(i -> importIndex(node, i));
			}
		}
		
		public void importLabels(Node node, Collection<String> labels) {
			if (null != labels) {
				labels.stream().map(l -> Label.label(l)).forEach(l -> node.addLabel(l));
			}
		}

		public void importProperties(Node node, Map<String, Object> properties) {
			if (null != properties) {
				properties.entrySet().stream().forEach(e -> node.setProperty(e.getKey(), e.getValue()));
			}
		}
		
		public void importProperties(Relationship relationship, Map<String, Object> properties) {
			if (null != properties) {
				properties.entrySet().stream().forEach(e -> relationship.setProperty(e.getKey(), e.getValue()));
			}
		}
		
		public void importRelationships(Collection<GraphRelationship> relationships, boolean storeUnknown) {
			if (null != relationships) {
				relationships.stream().forEach(r -> importRelationship(r, storeUnknown));
			}
		}
		
		public Node importNode(GraphNode graphNode) {
			if (graphNode.isBroken() || graphNode.isDeleted())
				return null;
			
			GraphKey key = graphNode.getKey();
			
			if (StringUtils.isEmpty(key.getLabel()))
				throw new IllegalArgumentException("Node Key Label can not be empty");
			if (StringUtils.isEmpty(key.getProperty()))
				throw new IllegalArgumentException("Node Key Property can not be null");
			if (null == key.getValue())
				throw new IllegalArgumentException("Node Key Value can not be null");
				
			if (verbose) {
				System.out.println("Importing Node (" + key + ")");
			}
			
			Node node = findAnyNode(key);
			if (null == node) {
				node = createNode();
				
				importIndex(node, key);
				importIndexes(node, graphNode.getIndexSet());
			} else  {
				++nodesUpdated;
			}
				
			importLabels(node, graphNode.getLabels());
			importProperties(node, graphNode.getProperties());
			
			return node;
		}
			
		public void importRelationship(GraphRelationship graphRelationship, boolean storeUnknown) {
			String relationshipName = graphRelationship.getRelationship();
			GraphKey start = graphRelationship.getStart();
			GraphKey end = graphRelationship.getEnd();
			
			List<Node> nodesStart = findAllNodes(start);
			if (nodesStart.isEmpty() && storeUnknown) { 
				storeUnknownRelationship(getRelationshipKey(start), graphRelationship);
				
				if (verbose)
					System.out.println("Relationship Start Key (" + start + ") does not exists");
			}
			
			List<Node> nodesEnd = findAllNodes(end);
			if (nodesEnd.isEmpty() && storeUnknown) {
				storeUnknownRelationship(getRelationshipKey(end), graphRelationship);
				
				if (verbose)
					System.out.println("Relationship End Key (" + end + ") does not exists");
			}
			
			if (nodesStart.isEmpty() || nodesEnd.isEmpty())
				return;
			
			if (verbose) 
				System.out.println("Importing Relationship (" + start + ")-[" + relationshipName + "]->(" + end + ")");
			
			RelationshipType relationshipType = RelationshipType.withName(relationshipName);
			nodesStart.stream().forEach(nodeStart -> {
				nodesEnd.stream().forEach(nodeEnd -> mergeRelationship(nodeStart, nodeEnd, relationshipType, 
						Direction.OUTGOING, graphRelationship.getProperties()));
			});
		}
		
			
		Neo4jTransaction(GraphDatabaseService graphDb) {
			this.graphDb = graphDb;
		}
		
		Transaction beginTx() {
			return graphDb.beginTx();
		}
		
		Result execute(String cypher) {
			return graphDb.execute(cypher);
			
		}
		
		ResourceIterable<Node> getAllNodes() {
			return graphDb.getAllNodes();
		}
		
		ResourceIterator<Node> findNodes(Label label) {
			return graphDb.findNodes(label);
		}
		
		ConstraintDefinition createConstrant(Label label, String key) {
			Schema schema = graphDb.schema();
			
			for (ConstraintDefinition constraint : schema.getConstraints(label))
				for (String property : constraint.getPropertyKeys())
					if (property.equals(key))
						return constraint;  // already existing
				
			return schema
					.constraintFor(label)
					.assertPropertyIsUnique(key)
					.create();
		}
		
		ConstraintDefinition createConstrant(String label, String key) {
			return createConstrant(Label.label(label), key);
		}
		
		IndexDefinition createIndex(Label label, String key) {
			Schema schema = graphDb.schema();
			
			for (IndexDefinition index : schema.getIndexes(label))
				for (String property : index.getPropertyKeys())
					if (property.equals(key))
						return index;  // already existing
				
			return schema
					.indexFor(label)
					.on(key)
					.create();
		}
		
		IndexDefinition createIndex(String label, String key) {
			return createIndex(Label.label(label), key);
		}
		
		
		
		Node findAnyNode(Label label, String key, Object value) {
			try (ResourceIterator<Node> nodes = graphDb.findNodes(label, key, value)) {
				if (!nodes.hasNext())
					return null;
				
				return nodes.next();
			}		
		}
		
		Node findAnyNode(String label, String key, Object value) {
			return findAnyNode(Label.label(label), key, value); 		
		}
		
		Node findAnyNode(GraphKey key) {
			return findAnyNode(key.getLabel(), key.getProperty(), key.getValue()); 		
		}	
		
		List<Node> findAllNodes(Label label, String key, Object value) {
			try (ResourceIterator<Node> hits = graphDb.findNodes(label, key, value)) {
				List<Node> nodes = new ArrayList<Node>();
				
				while (hits.hasNext()) {
					nodes.add(hits.next());
				}
				
				return nodes;
			}
		}
		
		List<Node> findAllNodes(String label, String key, Object value) {
			return findAllNodes(Label.label(label), key, value);
		}
		
		List<Node> findAllNodes(GraphKey key) {
			return findAllNodes(key.getLabel(), key.getProperty(), key.getValue());
		}
		
		Relationship findRelationship(Iterable<Relationship> rels, long nodeId, Direction direction) {
			for (Relationship rel : rels) {
				switch (direction) {
				case INCOMING:
					if (rel.getStartNode().getId() == nodeId)
						return rel;
					break;
				case OUTGOING:
					if (rel.getEndNode().getId() == nodeId)
						return rel;
					
				case BOTH:
					if (rel.getStartNode().getId() == nodeId || 
					    rel.getEndNode().getId() == nodeId)
						return rel;
				}
			}
			
			return null;
		}
		
		Relationship findRelationship(Node nodeStart, long nodeId, 
				RelationshipType type, Direction direction) {
			return findRelationship(nodeStart.getRelationships(type, direction), nodeId, direction);
		}
		
		Relationship findRelationship(Node nodeStart, Node endNode, 
				RelationshipType type, Direction direction) {
			return findRelationship(nodeStart, endNode.getId(), type, direction);
		}
		
		Node createNode() {
			++nodesCreated;
			
			return graphDb.createNode();
		}

		Relationship createRelationship(Node nodeStart, Node nodeEnd, RelationshipType type) {
			++relationshipsCreated;
			
			return nodeStart.createRelationshipTo(nodeEnd, type);		
		}

		void storeUnknownRelationship(String key, GraphRelationship relationship) {
			List<GraphRelationship> list = unknownRelationships.get(key);
			if (null == list) 
				unknownRelationships.put(key, list = new ArrayList<GraphRelationship>());
			
			list.add(relationship);
		}
		
		Relationship mergeRelationship(Node nodeStart, Node nodeEnd, RelationshipType type, 
				Direction direction, Map<String, Object> properties) {

			Relationship relationship = findRelationship(nodeStart, nodeEnd, type, direction);
			if (null == relationship) 
				relationship = createRelationship(nodeStart, nodeEnd, type);
			else 
				++relationshipsUpdated;
			
			importProperties(relationship, properties);
			
			return relationship;
		}
	}
	
	private static File GetDbPath(final String folder) throws Neo4jException, IOException
	{
		File db = new File(folder, NEO4J_DB);
		if (!db.exists())
			db.mkdirs();
				
		if (!db.isDirectory())
			throw new Neo4jException("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");
		
		return db;
	}
	
	private static File GetConfPath(final String folder) throws Neo4jException
	{
		File conf = new File(folder, NEO4J_CONF);
		if (!conf.exists() || conf.isDirectory())
			throw new Neo4jException("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");
		
		return conf;
	}	
	
	private static GraphDatabaseService getReadOnlyGraphDb( final String graphDbPath ) throws Neo4jException {
		if (StringUtils.isEmpty(graphDbPath))
			throw new Neo4jException("Please provide path to an existing Neo4j instance");
		
		try {
			GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(graphDbPath) )
				.loadPropertiesFromFile( GetConfPath(graphDbPath).toString() )
				.setConfig( GraphDatabaseSettings.read_only, "true" )
				.newGraphDatabase();
			
			registerShutdownHook( graphDb );
			
			return graphDb;
		} catch (Exception e) {
			throw new Neo4jException("Unable to open Neo4j instance located at: " + graphDbPath + ". Error: " + e.getMessage());
		}
	}
	
	
	private static GraphDatabaseService getGraphDb( final String graphDbPath ) throws Neo4jException {
		if (StringUtils.isEmpty(graphDbPath))
			throw new Neo4jException("Please provide path to an existing Neo4j instance");
		
		try {
			GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(graphDbPath) )
				.loadPropertiesFromFile( GetConfPath(graphDbPath).toString() )
				.newGraphDatabase();
		
			registerShutdownHook( graphDb );
		
			return graphDb;
		} catch (Exception e) {
			throw new Neo4jException("Unable to open Neo4j instance located at: " + graphDbPath + ". Error: " + e.getMessage());
		}
	}
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    });
	}
	
	private static String getRelationshipKey(GraphKey key) {
		return key.getLabel() + "." + key.getProperty() + "." + key.getValue();
	}
	
	public Neo4jDatabase(GraphDatabaseService graphDb) throws Exception {		
		tx = new Neo4jTransaction(graphDb);
	}

	public Neo4jDatabase(final String neo4jFolder) throws Exception {		
		this(getGraphDb( neo4jFolder ));
	}

	
	public Neo4jDatabase(final String neo4jFolder, boolean readOnly) throws Exception {
		this(readOnly ? getReadOnlyGraphDb(neo4jFolder) : getGraphDb( neo4jFolder ));
	}
		
	public boolean isVerbose() {
		return verbose;
	}

	public long getNodesCreated() {
		return nodesCreated;
	}

	public long getNodesUpdated() {
		return nodesUpdated;
	}

	public long getRelationshipsCreated() {
		return relationshipsCreated;
	}

	public long getRelationshipsUpdated() {
		return relationshipsUpdated;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	public void resetCounters() {
		nodesCreated = nodesUpdated = relationshipsCreated = relationshipsUpdated = 0;
	}
	
	public void printStatistics(PrintStream out) {
		out.println( String.format("%d nodes has been created.\n%d nodes has been updated.\n%d relationships has been created.\n%d relationships has been updated.\n%d relationships keys has been invalid.", 
				nodesCreated, nodesUpdated, relationshipsCreated, relationshipsUpdated, unknownRelationships.size()) );
	}
	
	public long getSourcesConnectionsCount(String source1, String source2) {
		try ( Transaction t = tx.beginTx() ) 
		{
			String cypher = "MATCH (n1:" + source1 + ")-[x]-(n2:" + source2 + ") RETURN COUNT (DISTINCT x) AS n";
			try (Result result = tx.execute(cypher)) {
				if  ( result.hasNext() )
			    {
			        Map<String,Object> row = result.next();
			        return (Long) row.get(COLUMN_N);
			    }
			}
		}
		
		return 0;
	}
	
	
	public void enumrateAllNodes(ProcessNode processNode) throws Exception {
		try ( Transaction t = tx.beginTx() ) 
		{
			ResourceIterable<Node> nodes = tx.getAllNodes();
			for (Node node : nodes) {
				if (!processNode.processNode(tx, node))
					break;
			}
		
			t.success();
		}
	}
	
	public void enumrateAllNodesWithLabel(Label label, ProcessNode processNode) throws Exception {
		try ( Transaction t = tx.beginTx() ) 
		{
			
			try (ResourceIterator<Node> nodes = tx.findNodes(label)) {
				while (nodes.hasNext()) {
					if (!processNode.processNode(tx, nodes.next()))
						break;
				}
			}
			
			t.success();
		}
	}
	
	public void enumrateAllNodesWithLabel(String label, ProcessNode processNode) throws Exception {
		enumrateAllNodesWithLabel(Label.label(label), processNode);
	}
	
	public void enumrateAllNodesWithProperty(String property, ProcessNode processNode) throws Exception {
		try ( Transaction t = tx.beginTx() ) 
		{
			String cypher = "MATCH (n) WHERE EXISTS(n." + property + ") RETURN n";
			try (Result result = tx.execute(cypher)) {
				while ( result.hasNext() )
			    {
			        Map<String,Object> row = result.next();
			        if (!processNode.processNode(tx, (Node) row.get(COLUMN_N)))
			        	break;
			    }
			}
			
			t.success();
		}
	}
	
	public void enumrateAllNodesWithLabelAndProperty(String label, String property, ProcessNode processNode)  throws Exception {
		try ( Transaction t = tx.beginTx() ) 
		{
			String cypher = "MATCH (n:" + label + ") WHERE EXISTS(n." + property + ") RETURN n";
			try (Result result = tx.execute(cypher)) {
				while ( result.hasNext() )
			    {
			        Map<String,Object> row = result.next();
			        if (!processNode.processNode(tx, (Node) row.get(COLUMN_N)))
			        	break;
			    }
			}
			
			t.success();
		}
	}
	
	public void enumrateAllNodesWithLabelAndProperty(Label label, String property, ProcessNode processNode)  throws Exception {
		enumrateAllNodesWithLabelAndProperty(label.toString(), property, processNode);
	}
	
	public ConstraintDefinition createConstrant(Label label, String key) {
		ConstraintDefinition def = null;
		
		try ( Transaction t = tx.beginTx() ) 
		{
			def = tx.createConstrant(label, key);
			
			t.success();
		}
		
		return def;
	}	
	
	public ConstraintDefinition createConstrant(String label, String key) {
		return createConstrant(Label.label(label), key);
	}
	
	public IndexDefinition createIndex(Label label, String key) {
		IndexDefinition def = null;
		
		try ( Transaction t = tx.beginTx() ) 
		{
			def = tx.createIndex(label, key);
			
			t.success();
		}
		
		return def;
	}
	
	public IndexDefinition createIndex(String label, String key) {
		return createIndex(Label.label(label), key);
	}
	
	public void importGraph(Graph graph) {
	
		// schema can not be imported in the same transaction as nodes and relationships
		try ( Transaction t = tx.beginTx() ) 
		{
			tx.importSchemas(graph.getSchemas());
			
			t.success();
		}
		
		try ( Transaction t = tx.beginTx() ) 
		{
			tx.importNodes(graph.getNodes());
			tx.importRelationships(graph.getRelationships(), true);
			
			t.success();
		}
	}
	
	public void importSchemas(Collection<GraphSchema> schemas) {
		try ( Transaction t = tx.beginTx() ) 
		{
			tx.importSchemas(schemas);
		
			t.success();
		}
	}

	public void importSchema(GraphSchema schema) {
		try ( Transaction t = tx.beginTx() ) 
		{
			tx.importSchema(schema);
		
			t.success();
		}
	}
	
	public void importNodes(Collection<GraphNode> nodes) {
		// Import nodes
		try ( Transaction t = tx.beginTx() ) 
		{
			tx.importNodes(nodes);		
				
			t.success();
		}
	}

	public void importNode(GraphNode node) {
		// Import nodes
		try ( Transaction t = tx.beginTx() ) 
		{
			tx.importNode(node);		
				
			t.success();
		}
	}
	
	public void importRelationships(Collection<GraphRelationship> relationships) {
		try ( Transaction t = tx.beginTx() ) 
		{		
			tx.importRelationships(relationships, true);
			
			t.success();
		}
	}
	
	public void importRelationship(GraphRelationship relationship) {
		try ( Transaction t = tx.beginTx() ) 
		{		
			tx.importRelationship(relationship, true);
			
			t.success();
		}
	}
	
	
}
