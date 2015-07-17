package com.marklogic.semantics.sesame.config;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.repository.config.RepositoryImplConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author James Fuller
 */
public class MarkLogicRepositoryFactory implements RepositoryFactory {

	protected final Logger logger = LoggerFactory.getLogger(MarkLogicRepositoryFactory.class);

	public static ValueFactory vf= new ValueFactoryImpl();

	public static final String REPOSITORY_TYPE = "marklogic:MarkLogicRepository";

	public String getRepositoryType() {
		return REPOSITORY_TYPE;
	}

	public RepositoryImplConfig getConfig() {
		return new MarkLogicRepositoryConfig();
	}

    @Override
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
