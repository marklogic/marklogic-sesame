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
package com.marklogic.semantics.sesame.config;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.repository.config.RepositoryImplConfig;
import org.openrdf.repository.sparql.SPARQLRepository;

/**
 * Creates {@link MarkLogicRepository} from a configuration.
 * 
 * @author James Fuller
 */
public class MarkLogicRepositoryFactory implements RepositoryFactory {

	public static final String REPOSITORY_TYPE = "marklogic:MarkLogicRepository";

	public String getRepositoryType() {
		return REPOSITORY_TYPE;
	}

	public RepositoryImplConfig getConfig() {
		return new MarkLogicRepositoryConfig();
	}

	public MarkLogicRepository getRepository(RepositoryImplConfig config)
		throws RepositoryConfigException
	{
		MarkLogicRepository result = null;

		if (config instanceof MarkLogicRepositoryConfig) {
			MarkLogicRepositoryConfig httpConfig = (MarkLogicRepositoryConfig)config;
			if (httpConfig.getUpdateEndpointUrl() != null) {
				result = new MarkLogicRepository(httpConfig.getQueryEndpointUrl(), httpConfig.getUpdateEndpointUrl());
			}
			else {
				result = new MarkLogicRepository(httpConfig.getQueryEndpointUrl());
			}
		}
		else {
			throw new RepositoryConfigException("Invalid configuration class: " + config.getClass());
		}
		return result;
	}
}
