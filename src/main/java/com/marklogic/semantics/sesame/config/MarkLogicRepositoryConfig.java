package com.marklogic.semantics.sesame.config;

import org.openrdf.model.Graph;
import org.openrdf.model.URI;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.util.GraphUtilException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryImplConfigBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author James Fuller
 */
public class MarkLogicRepositoryConfig extends RepositoryImplConfigBase {

	protected final Logger logger = LoggerFactory.getLogger(MarkLogicRepositoryConfig.class);

	public static ValueFactory vf= new ValueFactoryImpl();

	public static final URI QUERY_ENDPOINT = vf.createURI("http://www.openrdf.org/config/repository/sparql#query-endpoint");

	public static final URI UPDATE_ENDPOINT = vf.createURI("http://www.openrdf.org/config/repository/sparql#update-endpoint");

	private String queryEndpointUrl;
	private String updateEndpointUrl;

	private String host;
	private int port;
	private String user;
	private String password;
	private String auth;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getAuth() {
		return auth;
	}

	public void setAuth(String auth) {
		this.auth = auth;
	}
	public MarkLogicRepositoryConfig() {
		super(MarkLogicRepositoryFactory.REPOSITORY_TYPE);
	}

	public MarkLogicRepositoryConfig(String queryEndpointUrl) {
		setQueryEndpointUrl(queryEndpointUrl);
	}
	
	public MarkLogicRepositoryConfig(String queryEndpointUrl, String updateEndpointUrl) {
		this(queryEndpointUrl);
		setUpdateEndpointUrl(updateEndpointUrl);
	}

	public String getQueryEndpointUrl() {
		return queryEndpointUrl;
	}

	public void setQueryEndpointUrl(String url) {
		this.queryEndpointUrl = url;
	}

	public String getUpdateEndpointUrl() {
		return updateEndpointUrl;
	}

	public void setUpdateEndpointUrl(String url) {
		this.updateEndpointUrl = url;
	}
	
	@Override
	public void validate() throws RepositoryConfigException {
		super.validate();
		if (getQueryEndpointUrl() == null) {
			throw new RepositoryConfigException(
					"No endpoint URL specified for SPARQL repository");
		}
	}

	@Override
	public Resource export(Graph graph) {
		Resource implNode = super.export(graph);

		ValueFactory vf = graph.getValueFactory();
		if (getQueryEndpointUrl() != null) {
			graph.add(implNode, QUERY_ENDPOINT, vf.createURI(getQueryEndpointUrl()));
		}
		if (getUpdateEndpointUrl() != null) {
			graph.add(implNode, UPDATE_ENDPOINT, vf.createURI(getUpdateEndpointUrl()));
		}

		return implNode;
	}

	@Override
	public void parse(Graph graph, Resource implNode)
			throws RepositoryConfigException {
		super.parse(graph, implNode);

		try {
			URI uri = GraphUtil.getOptionalObjectURI(graph, implNode, QUERY_ENDPOINT);
			if (uri != null) {
				setQueryEndpointUrl(uri.stringValue());
			}
			
			uri = GraphUtil.getOptionalObjectURI(graph, implNode, UPDATE_ENDPOINT);
			if (uri != null) {
				setUpdateEndpointUrl(uri.stringValue());
			}
		} catch (GraphUtilException e) {
			throw new RepositoryConfigException(e.getMessage(), e);
		}
	}
}
