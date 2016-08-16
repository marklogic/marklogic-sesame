package com.marklogic.semantics.sesame;

// https://github.com/marklogic/marklogic-sesame/issues/282
//

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.semantics.GraphManager;

public class MultiThreadedPersistenceTest extends SesameTestBase {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    //create some data
    List<String> identifiers = asList(
            "subject1", "subject2", "subject3","subject4","subject5",
            "subject6","subject7","subject8","subject9","subject10",
            "subject11","subject12","subject13","subject14","subject15",
            "subject16", "subject17", "subject18","subject19","subject20",
            "subject21", "subject22", "subject23","subject24","subject25",
            "subject26", "subject27", "subject28","subject29","subject30",
            "subject31", "subject32", "subject33","subject34","subject35",
            "subject36", "subject37", "subject38","subject39","subject40");

    @After
    public void tearDown()
            throws Exception {
        logger.debug("tearing down...");
        GraphManager gmgr = adminClient.newGraphManager();
        gmgr.delete("http://graph/meta");
        for (String id : identifiers)
        {
            gmgr.delete("http://foo/graph/" + id);
        }
        logger.info("tearDown complete.");
    }

    @Test
    public void multiThreadedPersist() throws RepositoryException, InterruptedException {
        final PersistenceService persistenceService = new PersistenceService(SesameTestBase.host, SesameTestBase.port, SesameTestBase.adminUser, SesameTestBase.adminPassword, DatabaseClientFactory.Authentication.DIGEST.toString());

        //persist data with multiple threads against the persistence service - simulate multiple concurrent requests against a tomcat deployed ingestion service
        //results in intermittent MarkLogicTransactionExceptions in executor threads
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        try {
            for(final String identifier: identifiers) {
                for (int i=0; i<20; i++) {
                    executorService.submit(
                            new Runnable() {
                                @Override
                                public void run() {
                                    persistenceService.persist(entitiesFor(identifier));
                                }
                            }
                    );
                }
            }

            executorService.shutdown();
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } finally {
            if(!executorService.isTerminated()) {
                System.out.println("cancel non finished tasks");
            }
            executorService.shutdownNow();
            System.out.println("shut down finished");
        }
    }

    class PersistenceService {
        private MarkLogicRepository markLogicRepository;

        public PersistenceService(String host, int port, String user, String password, String digest) {
            markLogicRepository = new MarkLogicRepository(host, port, user, password, digest);
            try {
                markLogicRepository.initialize();
            } catch (RepositoryException e) {
                throw new RuntimeException("error initialising repo", e);
            }
        }

        public void persist(List<Entity> entities){

            MarkLogicRepositoryConnection connection = null;
            try {
                connection = markLogicRepository.getConnection();

                connection.begin();

                for(Entity e : entities) {
                    connection.add(e.getStatements(), e.getGraph());
                }

                connection.commit();

            } catch (RepositoryException e) {
                //print to sysout as thread exceptions are not propagated up to main thread
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }
    class Entity {
        private Collection<Statement> statements;
        private Resource[] graph;

        Entity(Collection<Statement> statements, Resource[] graph) {
            this.statements = statements;
            this.graph = graph;
        }

        public Collection<Statement> getStatements() {
            return statements;
        }

        public Resource[] getGraph() {
            return graph;
        }
    }

    private List<Entity> entitiesFor(String identifier) {
        ValueFactoryImpl vf = ValueFactoryImpl.getInstance();

        Collection<Statement> statements = asList(
                vf.createStatement(
                        vf.createURI("http://" + identifier),
                        vf.createURI("http://predicate/a"),
                        vf.createLiteral("object value a")
                ),
                vf.createStatement(
                        vf.createURI("http://" + identifier),
                        vf.createURI("http://predicate/b"),
                        vf.createLiteral("object value b")
                ),
                vf.createStatement(
                        vf.createURI("http://" + identifier),
                        vf.createURI("http://predicate/c"),
                        vf.createLiteral("object value c")
                )
        );
        Resource[] graphs = new Resource[]{vf.createURI("http://foo/graph/" + identifier)};
        Entity entity = new Entity(statements, graphs);


        Collection<Statement> graphStatements = asList(
                vf.createStatement(
                        vf.createURI("http://foo/graph/" + identifier),
                        vf.createURI("http://graph/timestamp"),
                        vf.createLiteral(System.currentTimeMillis())
                ),
                vf.createStatement(
                        vf.createURI("http://foo/graph/" + identifier),
                        vf.createURI("http://graph/version"),
                        vf.createLiteral("the graph version")
                )
        );
        Resource[] graphContext = new Resource[]{vf.createURI("http://graph/meta")};
        Entity entity1 = new Entity(graphStatements, graphContext);

        return asList(entity, entity1);
    }
}