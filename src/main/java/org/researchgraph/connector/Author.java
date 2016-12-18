package org.researchgraph.connector;

import org.researchgraph.graph.GraphKey;
import org.researchgraph.graph.GraphNode;
import org.researchgraph.graph.GraphUtils;

public class Author {

	private String firstName;
	private String lastName;
	private String fullName;
	private String orcid;
	
	public String getFirstName() {
		return firstName;
	}
	
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	
	public String getLastName() {
		return lastName;
	}
	
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	
	public String getFullName() {
		return fullName;
	}
	
	public void setFullName(String fullName) {
		this.fullName = fullName;
	}
	
	public String getOrcid() {
		return orcid;
	}
	
	public void setOrcid(String orcid) {
		this.orcid = orcid;
	}
	
	private String getKey(Work work) {
		return work.getDoi() + ":" + fullName;
	}
	
	
	public GraphNode toNode(Work work) {
		return GraphNode.builder()
				.withKey(new GraphKey(Connector.SOURCE_CROSSREF, getKey(work)))
				.withNodeSource(Connector.URL_CROSSREF)
				.withNodeType(GraphUtils.TYPE_RESEARCHER)
				.withLabel(Connector.SOURCE_CROSSREF)
				.withLabel(GraphUtils.TYPE_RESEARCHER)
				.withProperty(GraphUtils.PROPERTY_FIRST_NAME, firstName)
				.withProperty(GraphUtils.PROPERTY_LAST_NAME, lastName)
				.withProperty(GraphUtils.PROPERTY_FULL_NAME, fullName)
				.withProperty(GraphUtils.PROPERTY_ORCID_ID, orcid)
				.build();
	}
}
