/* 
 * Licensed to Aduna under one or more contributor license agreements.  
 * See the NOTICE.txt file distributed with this work for additional 
 * information regarding copyright ownership. 
 *
 * Aduna licenses this file to you under the terms of the Aduna BSD 
 * License (the "License"); you may not use this file except in compliance 
 * with the License. See the LICENSE.txt file distributed with this work 
 * for the full License.
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.marklogic.semantics.sesame.client;

import org.openrdf.http.client.BackgroundGraphResult;
import org.openrdf.http.client.QueueCursor;
import org.openrdf.model.Statement;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.rio.RDFParser;

import java.io.InputStream;
import java.nio.charset.Charset;

public class MarkLogicBackgroundGraphResult extends BackgroundGraphResult
{

	public MarkLogicBackgroundGraphResult(RDFParser parser, InputStream in, Charset charset, String baseURI) {
		super(parser, in, charset, baseURI);
	}

	public MarkLogicBackgroundGraphResult(QueueCursor<Statement> queue, RDFParser parser, InputStream in, Charset charset, String baseURI) {
		super(queue, parser, in, charset, baseURI);
	}

	@Override
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
