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
/**
 * A library that enables access to a MarkLogic-backed triple-store via the
 * Sesame API.
 */
package com.marklogic.semantics.sesame.config;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import org.openrdf.repository.Repository;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.repository.config.RepositoryImplConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * factory for generating MarkLogicRepository's
 *
 * @author James Fuller
 */
public class MarkLogicRepositoryFactory implements RepositoryFactory {

	protected final Logger logger = LoggerFactory.getLogger(MarkLogicRepositoryFactory.class);

	public static final String REPOSITORY_TYPE = "marklogic:MarkLogicRepository";

    @Override
    /**
     *
     */
    public String getRepositoryType() {
		return REPOSITORY_TYPE;
	}

    @Override
    /**
     *
     */
    public RepositoryImplConfig getConfig() {
        return new MarkLogicRepositoryConfig();
	}

    @Override
    /**
     *
     */
    public Repository getRepository(RepositoryImplConfig config) throws RepositoryConfigException {
        MarkLogicRepositoryConfig mlconfig = (MarkLogicRepositoryConfig) config;
        return new MarkLogicRepository(
                mlconfig.getHost(),
                mlconfig.getPort(),
                mlconfig.getUser(),
                mlconfig.getPassword(),
                mlconfig.getAuth());
    }
}
