package com.marklogic.semantics.sesame;

// https://github.com/marklogic/marklogic-sesame/issues/282
//

import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.StringHandle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.RepositoryException;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

public class MultiThreadedPersistenceTest {
    
    @After
    public void tearDown()
            throws Exception {

        String host = "localhost";
        int defaultPort = 8200;
        String user = "admin";
        String password = "admin";
        String auth = "DIGEST";

        MarkLogicRepository rep = new MarkLogicRepository(host, defaultPort, user, password, auth);
        rep.initialize();
        MarkLogicRepositoryConnection conn=rep.getConnection();
        conn.clear();
        conn.close();
        conn = null;
        rep.shutDown();
    }

    @Test
    public void multiThreadedPersist() throws RepositoryException, InterruptedException {
        final PersistenceService persistenceService = new PersistenceService();

        //create some data
        List<List<Entity>> entities = asList(
                entitiesFor("subject1"), entitiesFor("subject2"), entitiesFor("subject3"), entitiesFor("subject4"), entitiesFor("subject5"),
                entitiesFor("subject6"), entitiesFor("subject7"), entitiesFor("subject8"), entitiesFor("subject9"), entitiesFor("subject10"),
                entitiesFor("subject11"), entitiesFor("subject12"), entitiesFor("subject13"), entitiesFor("subject14"), entitiesFor("subject15")
        );

        //persist data with multiple threads against the persistence service - simulate multiple concurrent requests against a tomcat deployed ingestion service
        //results in intermittent MarkLogicTransactionExceptions in executor threads
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        try {
            for(final List<Entity> entity: entities) {
                executorService.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            persistenceService.persist(entity);
                        }
                    }
                );
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

        public PersistenceService() {
            String host = "localhost";
            int defaultPort = 8200;
            String user = "admin";
            String password = "admin";
            String auth = "DIGEST";

            markLogicRepository = new MarkLogicRepository(host, defaultPort, user, password, auth);
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
