package com.marklogic.semantics.sesame.client;

/**
 * Common interface for objects, such as Repository and RepositoryConnection,
 * that are dependent on {@link MarkLogicClient}.
 *
 * @author James Fuller
 */
public interface MarkLogicClientDependent {

    /**
     * {@link MarkLogicClient} that has been assigned or has been used by this object.
     *
     * @return an {@link MarkLogicClient} instance or null
     */
    MarkLogicClient getMarkLogicClient();

    /**
     * Assign an {@link MarkLogicClient} that this object should use.
     * Callers must ensure that the given client is properly closed elsewhere.
     *
     * @param client
     */
    void setMarkLogicClient(MarkLogicClient client);
}
