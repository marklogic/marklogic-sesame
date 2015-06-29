package com.marklogic.semantics.sesame.client;

import ch.qos.logback.classic.Logger;

import java.nio.charset.Charset;

public class MarkLogicSession implements MarkLogicClientDependent {

	/*-----------*
	 * Constants *
	 *-----------*/

	protected static final Charset UTF8 = Charset.forName("UTF-8");

	final Logger logger = null;

	private MarkLogicClient marklogicClient;

	/*-----------*
	 * Variables *
	 *-----------*/


	/*--------------*
	 * Constructors *
	 *--------------*/

	public MarkLogicSession(MarkLogicClient client) {
		this.marklogicClient = client;
	}

	/*-----------------*
	 * Get/set methods *
	 *-----------------*/

	public final MarkLogicClient getMarkLogicClient() {
		return marklogicClient;
	}

	public void setMarkLogicClient(MarkLogicClient marklogicClient) {
		this.marklogicClient = marklogicClient;
	}

}
