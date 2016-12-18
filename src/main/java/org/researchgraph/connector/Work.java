package org.researchgraph.connector;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.researchgraph.graph.Graph;
import org.researchgraph.graph.GraphKey;
import org.researchgraph.graph.GraphNode;
import org.researchgraph.graph.GraphUtils;

public class Work {
	private Long resolutionId;
	private String source;
	private String sourceUrl;
	private String doi;
	private String url;
	private String title;
	private Integer year;
	private Date created;
	private Date resolved;
	private final List<Author> authors = new ArrayList<Author>();
	
	public Work() {
		
	}
	
	public Work(String doi) {
		this.doi = doi;
	}
	
	public Long getResolutionId() {
		return resolutionId;
	}
	
	public void setResolutionId(Long resolutionId) {
		this.resolutionId = resolutionId;
	}
	
	public String getSource() {
		return source;
	}
	
	public void setSource(String source) {
		this.source = source;
	}
	
	public String getSourceUrl() {
		return sourceUrl;
	}
	
	public void setSourceUrl(String sourceUrl) {
		this.sourceUrl = sourceUrl;
	}

	public String getDoi() {
		return doi;
	}
	
	public void setDoi(String doi) {
		this.doi = doi;
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}

	public String getTitle() {
		return title;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public Integer getYear() {
		return year;
	}
	
	public void setYear(Integer year) {
		this.year = year;
	}
	
	public Date getCreated() {
		return created;
	}
	
	public void setCreated(Date created) {
		this.created = created;
	}
	
	public boolean isCreated() {
		return null != resolutionId && null != created;
	}
	
	public Date getResolved() {
		return resolved;
	}
	
	public void setResolved(Date resolved) {
		this.resolved = resolved;
	}
	
	public boolean isResolved() {
		return null != this.resolved;
	}
	
	public List<Author> getAuthors() {
		return authors;
	}
	
	public void addAuthor(Author author) {
		this.authors.add(author);
	}
	
	public GraphNode toNode() {
		return GraphNode.builder()
				.withKey(new GraphKey(Connector.SOURCE_CROSSREF, url))
				.withNodeSource(Connector.URL_CROSSREF)
				.withNodeType(GraphUtils.TYPE_PUBLICATION)
				.withLabel(Connector.SOURCE_CROSSREF)
				.withLabel(GraphUtils.TYPE_PUBLICATION)
				.withProperty(GraphUtils.PROPERTY_DOI, doi)
				.withProperty(GraphUtils.PROPERTY_URL, url)
				.withProperty(GraphUtils.PROPERTY_TITLE, title)
				.withProperty(GraphUtils.PROPERTY_PUBLISHED_YEAR, year)
				.build();
	}
}
