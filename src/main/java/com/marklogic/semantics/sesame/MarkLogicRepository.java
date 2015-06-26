package com.marklogic.semantics.sesame;

import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.base.RepositoryBase;

import java.io.File;

public class MarkLogicRepository extends RepositoryBase {

    public MarkLogicRepository() {
        super();
    }

    public MarkLogicRepository(String endpointUrl) {
    }

    @Override
    protected void initializeInternal() throws RepositoryException {

    }

    @Override
    protected void shutDownInternal() throws RepositoryException {

    }

    @Override
    public void setDataDir(File dataDir) {

    }

    @Override
    public File getDataDir() {
        return null;
    }

    @Override
    public boolean isWritable() throws RepositoryException {
        return false;
    }

    @Override
    public RepositoryConnection getConnection() throws RepositoryException {
        return null;
    }

    @Override
    public ValueFactory getValueFactory() {
        return null;
    }
}
