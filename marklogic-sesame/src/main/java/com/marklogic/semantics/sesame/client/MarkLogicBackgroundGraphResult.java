/*
 * Copyright 2015 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.semantics.sesame.client;

import org.openrdf.http.client.BackgroundGraphResult;
import org.openrdf.http.client.QueueCursor;
import org.openrdf.model.Statement;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.rio.RDFParser;

import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * wrapper on Sesame BackgroundGraphResult so we can handle errors
 *
 *
 */

class MarkLogicBackgroundGraphResult extends BackgroundGraphResult
{

	/**
	 *  constructor
	 *
	 * @param parser
	 * @param in
	 * @param charset
	 * @param baseURI
	 */
	public MarkLogicBackgroundGraphResult(RDFParser parser, InputStream in, Charset charset, String baseURI) {
		super(parser, in, charset, baseURI);
	}

	/**
	 * constructor
	 *
	 * @param queue
	 * @param parser
	 * @param in
	 * @param charset
	 * @param baseURI
	 */
	public MarkLogicBackgroundGraphResult(QueueCursor<Statement> queue, RDFParser parser, InputStream in, Charset charset, String baseURI) {
		super(queue, parser, in, charset, baseURI);
	}


	@Override
	/**
	 * wrap exception to return false instead of throwing error
	 *
	 */
	public boolean hasNext()
		throws QueryEvaluationException
	{
		try {
			return super.hasNext();
		}catch(Exception e){
			return false;
		}
	}
	
}
