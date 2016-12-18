package org.researchgraph.connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.researchgraph.graph.Graph;
import org.researchgraph.graph.GraphKey;
import org.researchgraph.graph.GraphNode;
import org.researchgraph.graph.GraphRelationship;
import org.researchgraph.graph.GraphSchema;
import org.researchgraph.graph.GraphUtils;
import org.researchgraph.neo4j.Neo4jDatabase;

public class Connector {
	public static final String SOURCE_CROSSREF = "crossref";
	public static final String URL_CROSSREF ="crossref.org";
	
	private final Neo4jDatabase neo4j;
	private final Connection conn;
	private final PreparedStatement requestWork;
	private final PreparedStatement selectWork;
	private final PreparedStatement selectAuthors;
	private int processedNodes = 0;
	
	public Connector(String neo4jFolder, String host, int port, String user, String password, String database) throws Exception {
		neo4j = new Neo4jDatabase(neo4jFolder);	   
		conn = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database + "?user=" + user + "&password=" + password);
		
		this.requestWork = conn.prepareStatement("INSERT INTO doi_resolution SET doi=?, created=NOW()");
		this.selectWork = conn.prepareStatement("SELECT resolution_id, source, source_url, url, title, year, created, resolved FROM doi_resolution WHERE doi LIKE ?");
		this.selectAuthors = conn.prepareStatement("SELECT first_name, last_name, full_name, orcid FROM doi_author WHERE resolution_id=?");
	}
	
	public void processNodes(String source, String property, String relationship) throws Exception {
		Map<String, Set<GraphKey>> references = new HashMap<String, Set<GraphKey>>();
		
		processedNodes  = 0;
		
		neo4j.createIndex(source, property);
		neo4j.enumrateAllNodesWithLabelAndProperty(source, property, (tx, node) -> {
			String keyValue = (String) node.getProperty(GraphUtils.PROPERTY_KEY);
			GraphKey key = new GraphKey(source, keyValue);
			Object dois = node.getProperty(property);
			
			if (dois instanceof String) {
			
				loadDOI(key, (String)dois, references); 	
			
			} else if (dois instanceof String[]) {
			
				for (String doi : (String[])dois)
					loadDOI(key, doi, references);
				
			}
			
			++processedNodes;
									
			return true;
		});
		
		System.out.println("Processed " + processedNodes + " nodes");
		
		processDOI(source, references);
	}
	
	private void loadDOI(GraphKey key, String ref, Map<String, Set<GraphKey>> references) {
		String doi = GraphUtils.extractDoi(ref);
		if (null != doi) {
			Set<GraphKey> ids = references.get(doi);
			if (null == ids) 
				references.put(doi, ids = new HashSet<GraphKey>());
			ids.add(key);					
		}
	}
	
	private void processDOI(String source, Map<String, Set<GraphKey>> references) throws SQLException {
		Graph graph = new Graph();
		
		graph.addSchema(new GraphSchema(source, GraphUtils.PROPERTY_KEY, true));
		graph.addSchema(new GraphSchema(source, GraphUtils.PROPERTY_DOI, false));
		graph.addSchema(new GraphSchema(source, GraphUtils.PROPERTY_URL, false));
		
		int chunks = 0;
		for (Map.Entry<String, Set<GraphKey>> entry : references.entrySet()) {
			
			Work work = loadWork(entry.getKey());
			
			if (work.isResolved()) {
				GraphNode workNode = work.toNode();
				
				graph.addNode(workNode);
				
				for (Author author : work.getAuthors()) {
					GraphNode authorNode = author.toNode(work);
					
					graph.addNode(authorNode);
					graph.addRelationship(createRelationship(authorNode.getKey(), workNode.getKey()));
				}
				
				for (GraphKey key : entry.getValue()) {
					graph.addRelationship(createRelationship(workNode.getKey(), key));
				}
				
				if (graph.getObjectsCount() >= 1000) {
					System.out.println("importing chunk: " + (++chunks));
				
					neo4j.importGraph(graph);
					graph = new Graph();
				}
			} else if (!work.isCreated()) {
				requestWork(work.getDoi());
			}
		}
		
		if (graph.getObjectsCount() > 0) {
			System.out.println("importing final chunk");
			neo4j.importGraph(graph);
		}
		
		neo4j.printStatistics(System.out);
	}
	
	private GraphRelationship createRelationship(GraphKey a, GraphKey b) { 
		return GraphRelationship.builder()
				.withRelationship(GraphUtils.RELATIONSHIP_RELATED_TO)
				.withStart(a)
				.withEnd(b)
				.build();
	}
	
	private Work loadWork(String doi) throws SQLException {
		Work work = new Work(doi);
				
		selectWork.setString(1, doi);
		try (ResultSet rsWork = selectWork.executeQuery()) {
			if (rsWork.next()) {
				work.setResolutionId(rsWork.getLong(1));
				work.setCreated(rsWork.getDate(7));
				work.setResolved(rsWork.getDate(8));
				
				if (work.isResolved()) {
					work.setSource(rsWork.getString(2));
					work.setSourceUrl(rsWork.getString(3));
					work.setUrl(rsWork.getString(4));
					work.setTitle(rsWork.getString(5));
					work.setYear(rsWork.getInt(6));
					
					selectAuthors.setLong(1, work.getResolutionId());
					try (ResultSet rsAuthor = selectAuthors.executeQuery()) {
						while (rsAuthor.next()) {
							Author author = new Author();
							
							author.setFirstName(rsAuthor.getString(1));
							author.setLastName(rsAuthor.getString(2));
							author.setFullName(rsAuthor.getString(3));
							author.setOrcid(rsAuthor.getString(4));
							
							work.addAuthor(author);
						}
					}
				}
			}
		}
		
		return work;
	}
	
	private boolean requestWork(String doi) throws SQLException {
		requestWork.setString(1, doi); 
		return requestWork.execute();
	}
}
